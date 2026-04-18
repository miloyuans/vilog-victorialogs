package query

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	"vilog-victorialogs/internal/service/cache"
	mongostore "vilog-victorialogs/internal/store/mongo"
)

type Service struct {
	store  *mongostore.Store
	cache  *cache.Service
	client *victorialogs.Client
	cfg    config.CacheConfig
	logger *zap.Logger
}

func New(store *mongostore.Store, cacheService *cache.Service, client *victorialogs.Client, cfg config.CacheConfig, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{
		store:  store,
		cache:  cacheService,
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

func (s *Service) Search(ctx context.Context, req model.SearchRequest) (model.SearchResponse, error) {
	startedAt := time.Now().UTC()
	normalized, start, end, _, pageSize, err := normalizeRequest(req)
	if err != nil {
		return model.SearchResponse{}, err
	}

	datasources, err := s.resolveDatasources(ctx, req.DatasourceIDs)
	if err != nil {
		return model.SearchResponse{}, err
	}
	tagDefinitions, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return model.SearchResponse{}, err
	}
	now := time.Now().UTC()

	s.logger.Info("search started",
		zap.Time("start", start),
		zap.Time("end", end),
		zap.Int("datasource_count", len(datasources)),
		zap.Int("requested_service_count", len(normalized.ServiceNames)),
		zap.Int("page_size", pageSize),
		zap.Bool("use_cache", normalized.UseCache),
	)

	type sourceResult struct {
		results []model.SearchResult
		status  model.QuerySourceStatus
		cacheHit bool
		partial bool
	}

	resultsCh := make(chan sourceResult, len(datasources))
	var wg sync.WaitGroup

	for _, datasource := range datasources {
		ds := datasource
		wg.Add(1)
		go func() {
			defer wg.Done()

			snapshot, _ := s.store.GetSnapshot(ctx, ds.ID)
			rows, status, cacheHit, sourcePartial, queryErr := s.searchDatasource(ctx, ds, snapshot, tagDefinitions, normalized, start, end, now)
			if queryErr != nil {
				resultsCh <- sourceResult{
					status: model.QuerySourceStatus{
						Datasource: ds.Name,
						Status:     "error",
						Hits:       0,
						Error:      queryErr.Error(),
					},
					cacheHit: false,
					partial:  true,
				}
				return
			}

			resultsCh <- sourceResult{
				results: rows,
				status:  status,
				cacheHit: cacheHit,
				partial: sourcePartial,
			}
		}()
	}

	wg.Wait()
	close(resultsCh)

	allResults := make([]model.SearchResult, 0)
	sourceStatuses := make([]model.QuerySourceStatus, 0, len(datasources))
	partial := false
	cacheHit := len(datasources) > 0
	for item := range resultsCh {
		allResults = append(allResults, item.results...)
		sourceStatuses = append(sourceStatuses, item.status)
		if item.status.Status == "error" || item.status.Status == "partial" || item.partial {
			partial = true
		}
		if !item.cacheHit {
			cacheHit = false
		}
	}
	if len(datasources) == 0 {
		cacheHit = false
	}

	sort.Slice(allResults, func(i, j int) bool {
		left := parseTimestamp(allResults[i].Timestamp)
		right := parseTimestamp(allResults[j].Timestamp)
		if left.Equal(right) {
			leftMessage := strings.ToLower(allResults[i].Message)
			rightMessage := strings.ToLower(allResults[j].Message)
			if leftMessage == rightMessage {
				if allResults[i].Datasource == allResults[j].Datasource {
					return allResults[i].Service < allResults[j].Service
				}
				return allResults[i].Datasource < allResults[j].Datasource
			}
			return leftMessage < rightMessage
		}
		return left.After(right)
	})

	if len(allResults) > pageSize {
		allResults = allResults[:pageSize]
	}
	response := model.SearchResponse{
		Results:  allResults,
		Sources:  sourceStatuses,
		Total:    countSourceHits(sourceStatuses),
		Partial:  partial,
		CacheHit: cacheHit,
		TookMS:   time.Since(startedAt).Milliseconds(),
	}
	s.logger.Info("search completed",
		zap.Int("total", response.Total),
		zap.Int("visible", len(response.Results)),
		zap.Bool("cache_hit", response.CacheHit),
		zap.Bool("partial", response.Partial),
		zap.Int64("took_ms", response.TookMS),
	)

	return response, nil
}

func (s *Service) searchDatasource(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	req model.SearchRequest,
	start, end, now time.Time,
) ([]model.SearchResult, model.QuerySourceStatus, bool, bool, error) {
	services, err := s.resolveServiceTargets(ctx, datasource.ID, req.ServiceNames)
	if err != nil {
		return nil, model.QuerySourceStatus{}, false, true, err
	}

	rows := make([]model.SearchResult, 0)
	cacheHit := true
	partial := false
	for _, serviceName := range services {
		serviceRows, serviceCacheHit, servicePartial, err := s.searchServiceWindow(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, now, req.UseCache)
		if err != nil {
			return nil, model.QuerySourceStatus{}, false, true, err
		}
		rows = append(rows, serviceRows...)
		cacheHit = cacheHit && serviceCacheHit
		partial = partial || servicePartial
	}

	return rows, model.QuerySourceStatus{
		Datasource: datasource.Name,
		Status:     statusLabelForRows(partial, len(rows) == 0),
		Hits:       len(rows),
	}, cacheHit, partial, nil
}

func (s *Service) searchServiceWindow(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end, now time.Time,
	useCache bool,
) ([]model.SearchResult, bool, bool, error) {
	rows := make([]model.SearchResult, 0)
	cacheHit := true
	partial := false
	for day := cacheDay(start); !day.After(cacheDay(end)); day = day.Add(24 * time.Hour) {
		partition, partitionCacheHit, partitionPartial, err := s.loadPartitionWindow(ctx, datasource, snapshot, tagDefinitions, serviceName, day, now, useCache)
		if err != nil {
			return nil, false, true, err
		}
		rows = append(rows, filterRowsByWindow(partition.Rows, start, end)...)
		cacheHit = cacheHit && partitionCacheHit
		partial = partial || partitionPartial || partition.Meta.Partial
	}
	return rows, cacheHit, partial, nil
}

func (s *Service) loadPartitionWindow(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	day, now time.Time,
	useCache bool,
) (cache.LocalLogPartition, bool, bool, error) {
	if useCache {
		partition, hit, err := s.cache.LoadLogPartition(day, serviceName, datasource)
		if err != nil {
			s.logger.Warn("load log partition failed",
				zap.Error(err),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
			)
		} else if hit {
			s.logger.Debug("log partition cache hit",
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
				zap.Int("rows", len(partition.Rows)),
			)
			if s.cache.LogPartitionNeedsRefresh(partition.Meta, day, now) {
				s.logger.Info("refreshing hot log partition",
					zap.String("datasource", datasource.Name),
					zap.String("service", displayServiceName(serviceName)),
					zap.String("day", cacheDay(day).Format("2006-01-02")),
					zap.Time("last_sync_at", partition.Meta.LastSyncAt),
				)
				refreshed, refreshPartial, refreshErr := s.refreshPartitionIncremental(ctx, datasource, snapshot, tagDefinitions, serviceName, day, now, partition)
				if refreshErr != nil {
					s.logger.Warn("refresh hot log partition failed, serving cached copy",
						zap.Error(refreshErr),
						zap.String("datasource", datasource.Name),
						zap.String("service", displayServiceName(serviceName)),
						zap.String("day", cacheDay(day).Format("2006-01-02")),
					)
					return partition, true, partition.Meta.Partial, nil
				}
				return refreshed, true, refreshPartial, nil
			}
			return partition, true, partition.Meta.Partial, nil
		}
	}

	s.logger.Info("building log partition from source",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.String("day", cacheDay(day).Format("2006-01-02")),
	)
	partition, partial, err := s.buildPartitionFromSource(ctx, datasource, snapshot, tagDefinitions, serviceName, day, now)
	if err != nil {
		return cache.LocalLogPartition{}, false, true, err
	}
	return partition, false, partial, nil
}

func (s *Service) buildPartitionFromSource(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	day, now time.Time,
) (cache.LocalLogPartition, bool, error) {
	rangeStart, rangeEnd := dayBounds(day, now)
	rows, partial, err := s.fetchCompleteSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, rangeStart, rangeEnd)
	if err != nil {
		return cache.LocalLogPartition{}, partial, err
	}
	partition := cache.LocalLogPartition{
		Meta: s.cache.PrepareLogPartitionMeta(day, serviceName, datasource, len(rows), now, partial),
		Rows: dedupeRows(rows),
	}
	if err := s.cache.StoreLogPartition(partition); err != nil {
		s.logger.Warn("store log partition failed",
			zap.Error(err),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
		)
	}
	return partition, partial, nil
}

func (s *Service) refreshPartitionIncremental(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	day, now time.Time,
	partition cache.LocalLogPartition,
) (cache.LocalLogPartition, bool, error) {
	rangeStart, rangeEnd := dayBounds(day, now)
	refreshStart := partition.Meta.LastSyncAt.Add(-2 * time.Second)
	if refreshStart.Before(rangeStart) {
		refreshStart = rangeStart
	}
	if !rangeEnd.After(refreshStart) {
		partition.Meta.LastSyncAt = now
		partition.Meta.LastAccessAt = now
		if err := s.cache.StoreLogPartition(partition); err != nil {
			s.logger.Warn("persist hot partition heartbeat failed",
				zap.Error(err),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
			)
		}
		return partition, partition.Meta.Partial, nil
	}

	rows, partial, err := s.fetchCompleteSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, refreshStart, rangeEnd)
	if err != nil {
		return cache.LocalLogPartition{}, true, err
	}
	partition.Rows = dedupeRows(append(partition.Rows, rows...))
	partition.Meta = s.cache.PrepareLogPartitionMeta(day, serviceName, datasource, len(partition.Rows), now, partition.Meta.Partial || partial)
	if err := s.cache.StoreLogPartition(partition); err != nil {
		s.logger.Warn("store refreshed hot partition failed",
			zap.Error(err),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
		)
	}
	return partition, partition.Meta.Partial, nil
}

func (s *Service) fetchCompleteSourceRange(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end time.Time,
) ([]model.SearchResult, bool, error) {
	return s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, 0)
}

func (s *Service) fetchSourceRangeRecursive(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end time.Time,
	depth int,
) ([]model.SearchResult, bool, error) {
	if !end.After(start) {
		return []model.SearchResult{}, false, nil
	}

	logsql := buildSourceLogsQL(datasource, snapshot, serviceName)
	rows, truncated, err := s.fetchDatasourceWindow(
		ctx,
		datasource,
		snapshot,
		tagDefinitions,
		logsql,
		start,
		end,
		s.cfg.SourceRequestLimit,
	)
	if err != nil {
		return nil, false, err
	}
	if !truncated {
		return dedupeRows(rows), false, nil
	}
	if end.Sub(start) <= time.Second {
		s.logger.Warn("source range truncated at minimum split window",
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.Time("start", start),
			zap.Time("end", end),
			zap.Int("depth", depth),
		)
		return dedupeRows(rows), true, nil
	}

	mid := start.Add(end.Sub(start) / 2)
	if !mid.After(start) || !end.After(mid) {
		return dedupeRows(rows), true, nil
	}
	s.logger.Debug("splitting truncated source window",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.Time("start", start),
		zap.Time("mid", mid),
		zap.Time("end", end),
		zap.Int("depth", depth),
	)
	leftRows, leftPartial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, start, mid, depth+1)
	if err != nil {
		return nil, false, err
	}
	rightRows, rightPartial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, mid, end, depth+1)
	if err != nil {
		return nil, false, err
	}
	return dedupeRows(append(leftRows, rightRows...)), leftPartial || rightPartial, nil
}

func (s *Service) resolveServiceTargets(ctx context.Context, datasourceID string, requested []string) ([]string, error) {
	if items := uniqueStrings(requested); len(items) > 0 {
		return items, nil
	}
	entries, err := s.store.ListServiceCatalog(ctx, datasourceID)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return []string{""}, nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.ServiceName)
	}
	return uniqueStrings(names), nil
}

func (s *Service) fetchDatasourceWindow(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	logsql string,
	start, end time.Time,
	targetLimit int,
) ([]model.SearchResult, bool, error) {
	requestedWindow := targetLimit
	if requestedWindow <= 0 {
		requestedWindow = 200
	}
	if requestedWindow > s.sourceWindowLimit() {
		requestedWindow = s.sourceWindowLimit()
	}

	normalizedRows := make([]model.SearchResult, 0, requestedWindow)
	offset := 0
	truncated := false
	lastBatchSize := 0
	lastBatchLimit := 0

	for offset < requestedWindow {
		limit := s.sourceChunkLimit()
		if remaining := requestedWindow - offset; remaining < limit {
			limit = remaining
		}
		lastBatchLimit = limit

		rows, err := s.client.Query(ctx, datasource, victorialogs.QueryRequest{
			Query:  logsql,
			Start:  start,
			End:    end,
			Limit:  limit,
			Offset: offset,
		})
		if err != nil {
			return nil, false, err
		}
		if len(rows) == 0 {
			break
		}
		lastBatchSize = len(rows)

		batch := make([]model.SearchResult, 0, len(rows))
		for _, row := range rows {
			batch = append(batch, normalizeRow(datasource, snapshot, tagDefinitions, row))
		}
		normalizedRows = mergeContinuationRows(normalizedRows, batch)
		offset += len(rows)

		if len(rows) < limit {
			break
		}
	}

	if offset >= requestedWindow && lastBatchSize >= lastBatchLimit {
		truncated = true
	}

	return normalizedRows, truncated, nil
}

func (s *Service) resolveDatasources(ctx context.Context, datasourceIDs []string) ([]model.Datasource, error) {
	if len(datasourceIDs) == 0 {
		return s.store.ListDatasources(ctx, true)
	}

	datasources := make([]model.Datasource, 0, len(datasourceIDs))
	for _, datasourceID := range datasourceIDs {
		ds, err := s.store.GetDatasource(ctx, datasourceID)
		if err != nil {
			return nil, err
		}
		if !ds.Enabled {
			continue
		}
		datasources = append(datasources, ds)
	}
	return datasources, nil
}

func normalizeRequest(req model.SearchRequest) (model.SearchRequest, time.Time, time.Time, int, int, error) {
	end, err := util.ParseTimeOrDefault(req.End, time.Now().UTC())
	if err != nil {
		return model.SearchRequest{}, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("invalid end time: %w", err)
	}
	start, err := util.ParseTimeOrDefault(req.Start, end.Add(-time.Hour))
	if err != nil {
		return model.SearchRequest{}, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("invalid start time: %w", err)
	}
	if start.After(end) {
		return model.SearchRequest{}, time.Time{}, time.Time{}, 0, 0, fmt.Errorf("start must be before end")
	}

	page := 1
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}
	if pageSize > 10000 {
		pageSize = 10000
	}

	normalized := req
	normalized.Start = start.Format(time.RFC3339)
	normalized.End = end.Format(time.RFC3339)
	normalized.Page = page
	normalized.PageSize = pageSize
	normalized.DatasourceIDs = uniqueStrings(req.DatasourceIDs)
	normalized.ServiceNames = uniqueStrings(req.ServiceNames)
	if normalized.Tags == nil {
		normalized.Tags = map[string][]string{}
	}
	for key, values := range normalized.Tags {
		normalized.Tags[key] = uniqueStrings(values)
	}

	return normalized, start, end, page, pageSize, nil
}

func buildLogsQL(datasource model.Datasource, snapshot model.DatasourceTagSnapshot, tags []model.TagDefinition, keyword string, serviceNames []string, tagFilters map[string][]string) string {
	parts := make([]string, 0)
	if strings.TrimSpace(keyword) != "" {
		parts = append(parts, quotePhrase(keyword))
	}

	serviceField := firstNonEmpty(datasource.FieldMapping.ServiceField, snapshot.ServiceField)
	if serviceField != "" && len(serviceNames) > 0 {
		if filter := buildExactFieldFilter(serviceField, serviceNames); filter != "" {
			parts = append(parts, filter)
		}
	}

	for tagName, values := range tagFilters {
		fieldName := resolveTagField(datasource.ID, tagName, tags)
		if fieldName == "" {
			fieldName = tagName
		}
		if filter := buildExactFieldFilter(fieldName, values); filter != "" {
			parts = append(parts, filter)
		}
	}

	if len(parts) == 0 {
		return "*"
	}
	return strings.Join(parts, " ")
}

func buildSourceLogsQL(datasource model.Datasource, snapshot model.DatasourceTagSnapshot, serviceName string) string {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return "*"
	}
	serviceField := firstNonEmpty(datasource.FieldMapping.ServiceField, snapshot.ServiceField, "service")
	return buildExactFieldFilter(serviceField, []string{serviceName})
}

func resolveTagField(datasourceID, tagName string, tags []model.TagDefinition) string {
	for _, tag := range tags {
		if !tag.Enabled || tag.Name != tagName {
			continue
		}
		if len(tag.DatasourceIDs) > 0 && !contains(tag.DatasourceIDs, datasourceID) {
			continue
		}
		return tag.FieldName
	}
	return ""
}

func buildExactFieldFilter(field string, values []string) string {
	filters := make([]string, 0, len(values))
	for _, value := range uniqueStrings(values) {
		filters = append(filters, fmt.Sprintf(`%s:=%s`, field, strconv.Quote(value)))
	}
	if len(filters) == 0 {
		return ""
	}
	if len(filters) == 1 {
		return filters[0]
	}
	return "(" + strings.Join(filters, " OR ") + ")"
}

func quotePhrase(value string) string {
	return strconv.Quote(strings.TrimSpace(value))
}

func normalizeRow(datasource model.Datasource, snapshot model.DatasourceTagSnapshot, tags []model.TagDefinition, row map[string]any) model.SearchResult {
	timestampField := firstNonEmpty(datasource.FieldMapping.TimeField, snapshot.TimeField, "_time")
	messageField := firstNonEmpty(datasource.FieldMapping.MessageField, snapshot.MessageField, "_msg")
	serviceField := firstNonEmpty(datasource.FieldMapping.ServiceField, snapshot.ServiceField)
	podField := firstNonEmpty(datasource.FieldMapping.PodField, snapshot.PodField)

	labels := make(map[string]string)
	for _, tag := range tags {
		if !tag.Enabled {
			continue
		}
		if len(tag.DatasourceIDs) > 0 && !contains(tag.DatasourceIDs, datasource.ID) {
			continue
		}
		if value, ok := row[tag.FieldName]; ok {
			labels[tag.Name] = stringify(value)
		}
	}

	return model.SearchResult{
		Timestamp:  formatTimestamp(extractValue(row, timestampField, "_time")),
		Message:    stringify(extractValue(row, messageField, "_msg", "message", "msg", "log")),
		Service:    stringify(extractValue(row, serviceField, "service", "service_name", "app", "job")),
		Pod:        stringify(extractValue(row, podField, "kubernetes.pod.name", "kubernetes_pod_name", "pod", "pod_name")),
		Datasource: datasource.Name,
		Labels:     labels,
		Raw:        row,
	}
}

func mergeContinuationRows(current []model.SearchResult, incoming []model.SearchResult) []model.SearchResult {
	if len(incoming) == 0 {
		return current
	}

	result := current
	for _, row := range incoming {
		if shouldMergeContinuation(result, row) {
			mergeIntoPrevious(&result[len(result)-1], row)
			continue
		}
		result = append(result, row)
	}
	return result
}

func shouldMergeContinuation(current []model.SearchResult, row model.SearchResult) bool {
	if len(current) == 0 {
		return false
	}
	previous := current[len(current)-1]
	if !sameResultSource(previous, row) {
		return false
	}

	message := strings.TrimSpace(row.Message)
	if message == "" {
		return true
	}
	if strings.TrimSpace(row.Timestamp) == "" {
		return true
	}
	if !looksLikeContinuationLine(message) {
		return false
	}
	if row.Timestamp == previous.Timestamp {
		return true
	}

	rowTime := parseTimestamp(row.Timestamp)
	previousTime := parseTimestamp(previous.Timestamp)
	if rowTime.IsZero() || previousTime.IsZero() {
		return false
	}
	delta := rowTime.Sub(previousTime)
	if delta < 0 {
		delta = -delta
	}
	return delta <= 2*time.Second
}

func mergeIntoPrevious(previous *model.SearchResult, continuation model.SearchResult) {
	if previous == nil {
		return
	}

	message := strings.TrimSpace(continuation.Message)
	if message != "" {
		if strings.TrimSpace(previous.Message) != "" {
			previous.Message += "\n"
		}
		previous.Message += message
	}
	if previous.Service == "" {
		previous.Service = continuation.Service
	}
	if previous.Pod == "" {
		previous.Pod = continuation.Pod
	}
	if previous.Labels == nil {
		previous.Labels = map[string]string{}
	}
	for key, value := range continuation.Labels {
		if previous.Labels[key] == "" && strings.TrimSpace(value) != "" {
			previous.Labels[key] = value
		}
	}
	if previous.Raw == nil {
		previous.Raw = map[string]any{}
	}
	existing, _ := previous.Raw["_merged_continuations"].([]any)
	previous.Raw["_merged_continuations"] = append(existing, continuation.Raw)
}

func statusLabelForRows(partial, empty bool) string {
	if partial {
		return "partial"
	}
	if empty {
		return "empty"
	}
	return "ok"
}

func sameResultSource(left, right model.SearchResult) bool {
	if left.Datasource != right.Datasource {
		return false
	}
	if strings.TrimSpace(left.Service) != "" && strings.TrimSpace(right.Service) != "" && left.Service != right.Service {
		return false
	}
	if strings.TrimSpace(left.Pod) != "" && strings.TrimSpace(right.Pod) != "" && left.Pod != right.Pod {
		return false
	}
	return true
}

func looksLikeContinuationLine(message string) bool {
	trimmed := strings.TrimSpace(message)
	lower := strings.ToLower(trimmed)
	if trimmed == "" {
		return false
	}
	switch {
	case strings.HasPrefix(lower, "at "):
		return true
	case strings.HasPrefix(lower, "caused by:"):
		return true
	case strings.HasPrefix(lower, "suppressed:"):
		return true
	case strings.HasPrefix(lower, "wrapped by:"):
		return true
	case strings.HasPrefix(lower, "... ") && strings.HasSuffix(lower, " more"):
		return true
	default:
		return false
	}
}

func (s *Service) sourceChunkLimit() int {
	chunkSize := s.cfg.SourceChunkSize
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	if s.cfg.SourceRequestLimit > 0 && chunkSize > s.cfg.SourceRequestLimit {
		chunkSize = s.cfg.SourceRequestLimit
	}
	if chunkSize <= 0 {
		return 1000
	}
	return chunkSize
}

func (s *Service) sourceWindowLimit() int {
	if s.cfg.MaxQueryWindow > 0 {
		return s.cfg.MaxQueryWindow
	}
	return 100000
}

func extractValue(row map[string]any, primary string, fallbacks ...string) any {
	if primary != "" {
		if value, ok := row[primary]; ok {
			return value
		}
	}
	for _, fallback := range fallbacks {
		if value, ok := row[fallback]; ok {
			return value
		}
	}
	return nil
}

func stringify(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func formatTimestamp(value any) string {
	switch typed := value.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, typed); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
		if parsed, err := time.Parse(time.RFC3339, typed); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
		return typed
	case float64:
		return numericTimeToRFC3339(typed)
	case int64:
		return numericTimeToRFC3339(float64(typed))
	case int:
		return numericTimeToRFC3339(float64(typed))
	default:
		return ""
	}
}

func numericTimeToRFC3339(value float64) string {
	switch {
	case value > 1e18:
		return time.Unix(0, int64(value)).UTC().Format(time.RFC3339)
	case value > 1e15:
		return time.UnixMicro(int64(value)).UTC().Format(time.RFC3339)
	case value > 1e12:
		return time.UnixMilli(int64(value)).UTC().Format(time.RFC3339)
	default:
		secs, frac := math.Modf(value)
		return time.Unix(int64(secs), int64(frac*float64(time.Second))).UTC().Format(time.RFC3339)
	}
}

func parseTimestamp(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func filterRowsByWindow(rows []model.SearchResult, start, end time.Time) []model.SearchResult {
	filtered := make([]model.SearchResult, 0, len(rows))
	for _, row := range rows {
		parsed := parseTimestamp(row.Timestamp)
		if !parsed.IsZero() && (parsed.Before(start) || parsed.After(end)) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func dedupeRows(rows []model.SearchResult) []model.SearchResult {
	seen := make(map[string]struct{}, len(rows))
	filtered := make([]model.SearchResult, 0, len(rows))
	for _, row := range rows {
		key := searchResultKey(row)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		filtered = append(filtered, row)
	}
	return filtered
}

func searchResultKey(row model.SearchResult) string {
	return strings.Join([]string{
		row.Timestamp,
		row.Datasource,
		row.Service,
		row.Pod,
		row.Message,
	}, "\x00")
}

func countSourceHits(items []model.QuerySourceStatus) int {
	total := 0
	for _, item := range items {
		total += item.Hits
	}
	return total
}

func cacheDay(value time.Time) time.Time {
	return time.Date(value.UTC().Year(), value.UTC().Month(), value.UTC().Day(), 0, 0, 0, 0, time.UTC)
}

func dayBounds(day, now time.Time) (time.Time, time.Time) {
	start := cacheDay(day)
	end := start.Add(24 * time.Hour)
	nowUTC := now.UTC()
	if cacheDay(nowUTC).Equal(start) && nowUTC.Before(end) {
		end = nowUTC
	}
	return start, end
}

func displayServiceName(serviceName string) string {
	if strings.TrimSpace(serviceName) == "" {
		return "__all__"
	}
	return serviceName
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
