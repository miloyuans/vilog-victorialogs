package discovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	"vilog-victorialogs/internal/service/cache"
	"vilog-victorialogs/internal/service/datasource"
	"vilog-victorialogs/internal/service/telegram"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

type Service struct {
	store         *mongostore.Store
	cache         *cache.Service
	datasources   *datasource.Service
	client        *victorialogs.Client
	telegram      *telegram.Service
	discoveryCfg  config.DiscoveryConfig
	victoriaCfg   config.VictoriaLogsConfig
}

func New(
	store *mongostore.Store,
	cacheService *cache.Service,
	datasourceService *datasource.Service,
	client *victorialogs.Client,
	telegramService *telegram.Service,
	discoveryCfg config.DiscoveryConfig,
	victoriaCfg config.VictoriaLogsConfig,
) *Service {
	return &Service{
		store:        store,
		cache:        cacheService,
		datasources:  datasourceService,
		client:       client,
		telegram:     telegramService,
		discoveryCfg: discoveryCfg,
		victoriaCfg:  victoriaCfg,
	}
}

func (s *Service) Discover(ctx context.Context, datasourceID, actor string) (model.DatasourceTagSnapshot, error) {
	ds, err := s.datasources.Get(ctx, datasourceID)
	if err != nil {
		return model.DatasourceTagSnapshot{}, err
	}

	end := time.Now().UTC()
	start := end.Add(-s.discoveryCfg.Window)

	streamFields, err := s.client.StreamFieldNames(ctx, ds, victorialogs.ListRequest{
		Query: "*",
		Start: start,
		End:   end,
		Limit: s.victoriaCfg.DiscoveryLimit,
	})
	if err != nil {
		return model.DatasourceTagSnapshot{}, err
	}

	fields, err := s.client.FieldNames(ctx, ds, victorialogs.ListRequest{
		Query: "*",
		Start: start,
		End:   end,
		Limit: s.victoriaCfg.DiscoveryLimit,
	})
	if err != nil {
		return model.DatasourceTagSnapshot{}, err
	}

	facets, err := s.client.Facets(ctx, ds, victorialogs.ListRequest{
		Query: "*",
		Start: start,
		End:   end,
		Limit: 5,
	})
	if err != nil {
		return model.DatasourceTagSnapshot{}, err
	}

	streamFieldNames := flattenValueNames(streamFields)
	fieldNames := flattenValueNames(fields)
	allFields := uniqueStrings(append(append([]string{}, streamFieldNames...), fieldNames...))

	serviceField := pickCandidate(firstNonEmpty(ds.FieldMapping.ServiceField), allFields, []string{
		"app", "service", "service_name", "job", "kubernetes.container.name", "kubernetes_container_name", "container", "container_name",
	})
	podField := pickCandidate(firstNonEmpty(ds.FieldMapping.PodField), allFields, []string{
		"kubernetes.pod.name", "kubernetes_pod_name", "pod", "pod_name",
	})
	messageField := pickCandidate(firstNonEmpty(ds.FieldMapping.MessageField), allFields, []string{
		"_msg", "message", "msg", "log",
	})
	timeField := pickCandidate(firstNonEmpty(ds.FieldMapping.TimeField), allFields, []string{
		"_time",
	})
	if timeField == "" {
		timeField = "_time"
	}

	tagCandidates, autoTags := inferTagDefinitions(ds.ID, allFields)
	highCardinalityFields := detectHighCardinality(allFields)
	serviceNames := []string{}
	if serviceField != "" {
		values, valueErr := s.lookupFieldValues(ctx, ds, serviceField, streamFieldNames, "*", start, end)
		if valueErr == nil {
			serviceNames = flattenValueNames(values)
		}
	}

	snapshot := model.DatasourceTagSnapshot{
		ID:                    util.NewPrefixedID("snap"),
		DatasourceID:          ds.ID,
		DiscoveredAt:          time.Now().UTC(),
		ServiceField:          serviceField,
		PodField:              podField,
		MessageField:          messageField,
		TimeField:             timeField,
		TagCandidates:         tagCandidates,
		HighCardinalityFields: highCardinalityFields,
		NotifyStatus:          "skip",
	}

	if err := s.store.UpsertSnapshot(ctx, snapshot); err != nil {
		return model.DatasourceTagSnapshot{}, err
	}
	if err := s.store.ReplaceServiceCatalog(ctx, ds.ID, serviceField, serviceNames, s.cache.ServiceListTTL()); err != nil {
		return model.DatasourceTagSnapshot{}, err
	}

	for _, tag := range autoTags {
		if err := s.store.UpsertAutoTagDefinition(ctx, tag); err != nil {
			return model.DatasourceTagSnapshot{}, err
		}
	}

	updatedMapping := ds.FieldMapping
	if strings.TrimSpace(updatedMapping.ServiceField) == "" {
		updatedMapping.ServiceField = serviceField
	}
	if strings.TrimSpace(updatedMapping.PodField) == "" {
		updatedMapping.PodField = podField
	}
	if strings.TrimSpace(updatedMapping.MessageField) == "" {
		updatedMapping.MessageField = messageField
	}
	if strings.TrimSpace(updatedMapping.TimeField) == "" {
		updatedMapping.TimeField = timeField
	}
	if updatedMapping != ds.FieldMapping {
		ds.FieldMapping = updatedMapping
		ds.UpdatedAt = time.Now().UTC()
		if err := s.store.UpdateDatasource(ctx, ds); err != nil {
			return model.DatasourceTagSnapshot{}, err
		}
	}

	if s.telegram != nil && s.telegram.Enabled() {
		if err := s.telegram.SendMessage(ctx, buildTelegramMessage(ds, snapshot, serviceNames, facets)); err == nil {
			snapshot.NotifyStatus = "telegram"
			_ = s.store.UpsertSnapshot(ctx, snapshot)
		} else {
			snapshot.NotifyStatus = "log"
			_ = s.store.UpsertSnapshot(ctx, snapshot)
		}
	}

	_ = s.store.CreateAuditLog(ctx, model.AuditLog{
		ID:           util.NewPrefixedID("audit"),
		ResourceType: "discovery",
		ResourceID:   ds.ID,
		Action:       "discover",
		Actor:        actor,
		Payload: map[string]any{
			"service_field": serviceField,
			"pod_field":     podField,
			"message_field": messageField,
			"time_field":    timeField,
		},
		CreatedAt: time.Now().UTC(),
	})

	return snapshot, nil
}

func (s *Service) RunStartupDiscovery(ctx context.Context) {
	datasources, err := s.datasources.ListEnabled(ctx)
	if err != nil {
		return
	}

	sem := make(chan struct{}, s.discoveryCfg.Concurrency)
	var wg sync.WaitGroup
	for _, datasource := range datasources {
		ds := datasource
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			_, _ = s.Discover(ctx, ds.ID, "startup")
		}()
	}
	wg.Wait()
}

func (s *Service) ListServices(ctx context.Context, datasourceID string) ([]string, bool, error) {
	cacheKey := datasourceID
	var cached model.ServiceListResponse
	cacheHit, err := s.cache.Get(ctx, cache.KindServiceList, cacheKey, &cached)
	if err == nil && cacheHit {
		return cached.Services, true, nil
	}

	if _, err := s.datasources.Get(ctx, datasourceID); err != nil {
		return nil, false, err
	}

	entries, err := s.store.ListServiceCatalog(ctx, datasourceID)
	if err != nil {
		return nil, false, err
	}
	discoveryFailed := false
	if len(entries) == 0 {
		if _, err := s.Discover(ctx, datasourceID, "api"); err == nil {
			entries, err = s.store.ListServiceCatalog(ctx, datasourceID)
			if err != nil {
				return nil, false, err
			}
		} else if errors.Is(err, mongostore.ErrNotFound) {
			return nil, false, err
		} else {
			discoveryFailed = true
		}
	}

	services := make([]string, 0, len(entries))
	for _, entry := range entries {
		services = append(services, entry.ServiceName)
	}
	services = uniqueStrings(services)
	sort.Strings(services)

	if !discoveryFailed {
		_ = s.cache.Set(ctx, cache.KindServiceList, cacheKey, model.ServiceListResponse{
			Services: services,
			CacheHit: false,
		}, s.cache.ServiceListTTL())
	}

	return services, false, nil
}

func (s *Service) ListTags(ctx context.Context, datasourceID, serviceName string) ([]model.TagDefinition, error) {
	tags, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	filtered := make([]model.TagDefinition, 0, len(tags))
	for _, tag := range tags {
		if !tag.Enabled {
			continue
		}
		if len(tag.DatasourceIDs) > 0 && !contains(tag.DatasourceIDs, datasourceID) {
			continue
		}
		if len(tag.ServiceNames) > 0 && (serviceName == "" || !contains(tag.ServiceNames, serviceName)) {
			continue
		}
		filtered = append(filtered, tag)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Priority == filtered[j].Priority {
			return filtered[i].Name < filtered[j].Name
		}
		return filtered[i].Priority > filtered[j].Priority
	})
	return filtered, nil
}

func (s *Service) ListAllTags(ctx context.Context) ([]model.TagDefinition, error) {
	return s.store.ListTagDefinitions(ctx)
}

func (s *Service) GetSnapshot(ctx context.Context, datasourceID string) (model.DatasourceTagSnapshot, error) {
	return s.store.GetSnapshot(ctx, datasourceID)
}

func (s *Service) ListTagValues(ctx context.Context, datasourceID, field, serviceName string) ([]string, bool, error) {
	cacheKey := datasourceID + ":" + field + ":" + serviceName
	var cached model.TagValuesResponse
	cacheHit, err := s.cache.Get(ctx, cache.KindTagValues, cacheKey, &cached)
	if err == nil && cacheHit {
		return cached.Values, true, nil
	}

	ds, err := s.datasources.Get(ctx, datasourceID)
	if err != nil {
		return nil, false, err
	}

	snapshot, _ := s.store.GetSnapshot(ctx, datasourceID)
	tags, _ := s.store.ListTagDefinitions(ctx)
	fieldName := resolveRequestedField(datasourceID, field, tags)

	end := time.Now().UTC()
	start := end.Add(-s.discoveryCfg.Window)
	query := "*"
	if serviceName != "" && snapshot.ServiceField != "" {
		query = fmt.Sprintf(`%s:=%q`, snapshot.ServiceField, serviceName)
	}

	streamFieldStats, _ := s.client.StreamFieldNames(ctx, ds, victorialogs.ListRequest{
		Query: query,
		Start: start,
		End:   end,
		Limit: s.victoriaCfg.DiscoveryLimit,
	})
	streamFields := flattenValueNames(streamFieldStats)

	values, err := s.lookupFieldValues(ctx, ds, fieldName, streamFields, query, start, end)
	if err != nil {
		return nil, false, err
	}
	items := flattenValueNames(values)
	items = uniqueStrings(items)
	sort.Strings(items)

	_ = s.cache.Set(ctx, cache.KindTagValues, cacheKey, model.TagValuesResponse{
		Field:    field,
		Values:   items,
		CacheHit: false,
	}, s.cache.TagValuesTTL())

	return items, false, nil
}

func (s *Service) CreateTag(ctx context.Context, req model.TagDefinitionUpsertRequest, actor string) (model.TagDefinition, error) {
	tag, err := buildTagDefinition("", req, time.Time{})
	if err != nil {
		return model.TagDefinition{}, err
	}
	now := time.Now().UTC()
	tag.ID = util.NewPrefixedID("tag")
	tag.CreatedAt = now
	tag.UpdatedAt = now

	if err := s.store.CreateTagDefinition(ctx, tag); err != nil {
		return model.TagDefinition{}, err
	}
	_ = s.store.CreateAuditLog(ctx, model.AuditLog{
		ID:           util.NewPrefixedID("audit"),
		ResourceType: "tag",
		ResourceID:   tag.ID,
		Action:       "create",
		Actor:        actor,
		Payload:      map[string]any{"name": tag.Name, "field_name": tag.FieldName},
		CreatedAt:    now,
	})
	return tag, nil
}

func (s *Service) UpdateTag(ctx context.Context, id string, req model.TagDefinitionUpsertRequest, actor string) (model.TagDefinition, error) {
	existing, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return model.TagDefinition{}, err
	}
	var current model.TagDefinition
	found := false
	for _, tag := range existing {
		if tag.ID == id {
			current = tag
			found = true
			break
		}
	}
	if !found {
		return model.TagDefinition{}, mongostore.ErrNotFound
	}

	tag, err := buildTagDefinition(id, req, current.CreatedAt)
	if err != nil {
		return model.TagDefinition{}, err
	}
	tag.UpdatedAt = time.Now().UTC()

	if err := s.store.UpdateTagDefinition(ctx, tag); err != nil {
		return model.TagDefinition{}, err
	}
	_ = s.store.CreateAuditLog(ctx, model.AuditLog{
		ID:           util.NewPrefixedID("audit"),
		ResourceType: "tag",
		ResourceID:   tag.ID,
		Action:       "update",
		Actor:        actor,
		Payload:      map[string]any{"name": tag.Name, "field_name": tag.FieldName},
		CreatedAt:    time.Now().UTC(),
	})
	return tag, nil
}

func (s *Service) DeleteTag(ctx context.Context, id, actor string) error {
	if err := s.store.DeleteTagDefinition(ctx, id); err != nil {
		return err
	}
	return s.store.CreateAuditLog(ctx, model.AuditLog{
		ID:           util.NewPrefixedID("audit"),
		ResourceType: "tag",
		ResourceID:   id,
		Action:       "delete",
		Actor:        actor,
		CreatedAt:    time.Now().UTC(),
	})
}

func (s *Service) lookupFieldValues(ctx context.Context, ds model.Datasource, field string, streamFields []string, query string, start, end time.Time) ([]victorialogs.ValueStat, error) {
	isStreamField := contains(streamFields, field)
	req := victorialogs.FieldValuesRequest{
		Query: query,
		Field: field,
		Start: start,
		End:   end,
		Limit: s.victoriaCfg.TagValueLimit,
	}
	if isStreamField {
		return s.client.StreamFieldValues(ctx, ds, req)
	}
	return s.client.FieldValues(ctx, ds, req)
}

func buildTagDefinition(id string, req model.TagDefinitionUpsertRequest, createdAt time.Time) (model.TagDefinition, error) {
	if strings.TrimSpace(req.Name) == "" {
		return model.TagDefinition{}, fmt.Errorf("name is required")
	}
	if strings.TrimSpace(req.FieldName) == "" {
		return model.TagDefinition{}, fmt.Errorf("field_name is required")
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	multi := true
	if req.Multi != nil {
		multi = *req.Multi
	}
	autoDiscovered := false
	if req.AutoDiscovered != nil {
		autoDiscovered = *req.AutoDiscovered
	}
	uiType := req.UIType
	if uiType == "" {
		uiType = "select"
	}
	priority := req.Priority
	if priority == 0 {
		priority = 100
	}

	return model.TagDefinition{
		ID:             id,
		Name:           strings.TrimSpace(req.Name),
		DisplayName:    firstNonEmpty(req.DisplayName, req.Name),
		FieldName:      strings.TrimSpace(req.FieldName),
		UIType:         uiType,
		Multi:          multi,
		Enabled:        enabled,
		DatasourceIDs:  uniqueStrings(req.DatasourceIDs),
		ServiceNames:   uniqueStrings(req.ServiceNames),
		AutoDiscovered: autoDiscovered,
		Priority:       priority,
		CreatedAt:      createdAt,
	}, nil
}

func resolveRequestedField(datasourceID, requested string, tags []model.TagDefinition) string {
	for _, tag := range tags {
		if !tag.Enabled {
			continue
		}
		if len(tag.DatasourceIDs) > 0 && !contains(tag.DatasourceIDs, datasourceID) {
			continue
		}
		if tag.Name == requested || tag.FieldName == requested {
			return tag.FieldName
		}
	}
	return requested
}

func flattenValueNames(values []victorialogs.ValueStat) []string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		if value.Value != "" {
			items = append(items, value.Value)
		}
	}
	return uniqueStrings(items)
}

func inferTagDefinitions(datasourceID string, fields []string) ([]string, []model.TagDefinition) {
	candidates := []struct {
		name       string
		displayName string
		fields     []string
	}{
		{name: "service", displayName: "service", fields: []string{"app", "service", "service_name", "job"}},
		{name: "namespace", displayName: "namespace", fields: []string{"namespace", "kubernetes.namespace", "kubernetes_namespace", "kubernetes.pod_namespace"}},
		{name: "pod", displayName: "pod", fields: []string{"kubernetes.pod.name", "kubernetes_pod_name", "pod", "pod_name"}},
		{name: "container", displayName: "container", fields: []string{"container", "container_name", "kubernetes.container.name", "kubernetes_container_name"}},
		{name: "level", displayName: "level", fields: []string{"level", "log.level", "severity", "severity_text", "lvl"}},
		{name: "host", displayName: "host", fields: []string{"host", "hostname", "instance", "node", "kubernetes.node.name", "kubernetes_node_name"}},
	}

	tagCandidates := make([]string, 0, len(candidates))
	autoTags := make([]model.TagDefinition, 0, len(candidates))
	now := time.Now().UTC()

	for _, candidate := range candidates {
		fieldName := pickCandidate("", fields, candidate.fields)
		if fieldName == "" {
			continue
		}
		tagCandidates = append(tagCandidates, candidate.name)
		autoTags = append(autoTags, model.TagDefinition{
			ID:             util.NewPrefixedID("tag"),
			Name:           candidate.name,
			DisplayName:    candidate.displayName,
			FieldName:      fieldName,
			UIType:         "select",
			Multi:          true,
			Enabled:        true,
			DatasourceIDs:  []string{datasourceID},
			ServiceNames:   []string{},
			AutoDiscovered: true,
			Priority:       100,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	return uniqueStrings(tagCandidates), autoTags
}

func detectHighCardinality(fields []string) []string {
	matches := make([]string, 0)
	for _, field := range fields {
		lower := strings.ToLower(field)
		if strings.Contains(lower, "trace_id") || strings.Contains(lower, "request_id") || strings.Contains(lower, "user_id") || lower == "ip" || strings.HasSuffix(lower, ".ip") {
			matches = append(matches, field)
		}
	}
	return uniqueStrings(matches)
}

func buildTelegramMessage(ds model.Datasource, snapshot model.DatasourceTagSnapshot, services []string, facets []victorialogs.Facet) string {
	lines := []string{
		fmt.Sprintf("Datasource: %s", ds.Name),
		fmt.Sprintf("Service field: %s", firstNonEmpty(snapshot.ServiceField, "-")),
		fmt.Sprintf("Pod field: %s", firstNonEmpty(snapshot.PodField, "-")),
		fmt.Sprintf("Message field: %s", firstNonEmpty(snapshot.MessageField, "-")),
		fmt.Sprintf("Time field: %s", firstNonEmpty(snapshot.TimeField, "-")),
		fmt.Sprintf("Tag candidates: %s", strings.Join(snapshot.TagCandidates, ", ")),
		fmt.Sprintf("High cardinality: %s", strings.Join(snapshot.HighCardinalityFields, ", ")),
		fmt.Sprintf("Services: %s", strings.Join(limitStrings(services, 20), ", ")),
	}
	if len(facets) > 0 {
		lines = append(lines, fmt.Sprintf("Facet fields discovered: %d", len(facets)))
	}
	return strings.Join(lines, "\n")
}

func pickCandidate(current string, fields, candidates []string) string {
	if current != "" {
		return current
	}
	for _, candidate := range candidates {
		for _, field := range fields {
			if strings.EqualFold(field, candidate) {
				return field
			}
		}
	}
	return ""
}

func uniqueStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func limitStrings(values []string, max int) []string {
	if len(values) <= max {
		return values
	}
	return values[:max]
}
