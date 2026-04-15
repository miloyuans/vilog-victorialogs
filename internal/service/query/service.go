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

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/model"
	"vilog-victorialogs/internal/service/cache"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

type Service struct {
	store  *mongostore.Store
	cache  *cache.Service
	client *victorialogs.Client
}

func New(store *mongostore.Store, cacheService *cache.Service, client *victorialogs.Client) *Service {
	return &Service{
		store:  store,
		cache:  cacheService,
		client: client,
	}
}

func (s *Service) Search(ctx context.Context, req model.SearchRequest) (model.SearchResponse, error) {
	startedAt := time.Now()
	normalized, start, end, page, pageSize, err := normalizeRequest(req)
	if err != nil {
		return model.SearchResponse{}, err
	}

	cacheKey, err := util.HashJSON(normalized)
	if err != nil {
		return model.SearchResponse{}, err
	}

	if req.UseCache {
		var cached model.SearchResponse
		cacheHit, cacheErr := s.cache.Get(ctx, cache.KindQuery, cacheKey, &cached)
		if cacheErr == nil && cacheHit {
			cached.CacheHit = true
			return cached, nil
		}
	}

	datasources, err := s.resolveDatasources(ctx, req.DatasourceIDs)
	if err != nil {
		return model.SearchResponse{}, err
	}
	tagDefinitions, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return model.SearchResponse{}, err
	}

	type sourceResult struct {
		results []model.SearchResult
		status  model.QuerySourceStatus
	}

	remoteLimit := page * pageSize
	resultsCh := make(chan sourceResult, len(datasources))
	var wg sync.WaitGroup

	for _, datasource := range datasources {
		ds := datasource
		wg.Add(1)
		go func() {
			defer wg.Done()

			snapshot, _ := s.store.GetSnapshot(ctx, ds.ID)
			logsql := buildLogsQL(ds, snapshot, tagDefinitions, req.Keyword, req.ServiceNames, req.Tags)
			rows, queryErr := s.client.Query(ctx, ds, victorialogs.QueryRequest{
				Query:  logsql,
				Start:  start,
				End:    end,
				Limit:  remoteLimit,
				Offset: 0,
			})
			if queryErr != nil {
				resultsCh <- sourceResult{
					status: model.QuerySourceStatus{
						Datasource: ds.Name,
						Status:     "error",
						Hits:       0,
						Error:      queryErr.Error(),
					},
				}
				return
			}

			if len(rows) == 0 {
				resultsCh <- sourceResult{
					results: []model.SearchResult{},
					status: model.QuerySourceStatus{
						Datasource: ds.Name,
						Status:     "empty",
						Hits:       0,
					},
				}
				return
			}

			normalizedRows := make([]model.SearchResult, 0, len(rows))
			for _, row := range rows {
				normalizedRows = append(normalizedRows, normalizeRow(ds, snapshot, tagDefinitions, row))
			}

			resultsCh <- sourceResult{
				results: normalizedRows,
				status: model.QuerySourceStatus{
					Datasource: ds.Name,
					Status:     "ok",
					Hits:       len(normalizedRows),
				},
			}
		}()
	}

	wg.Wait()
	close(resultsCh)

	allResults := make([]model.SearchResult, 0)
	sourceStatuses := make([]model.QuerySourceStatus, 0, len(datasources))
	partial := false
	for item := range resultsCh {
		allResults = append(allResults, item.results...)
		sourceStatuses = append(sourceStatuses, item.status)
		if item.status.Status == "error" {
			partial = true
		}
	}

	sort.Slice(allResults, func(i, j int) bool {
		left := parseTimestamp(allResults[i].Timestamp)
		right := parseTimestamp(allResults[j].Timestamp)
		if left.Equal(right) {
			return allResults[i].Datasource < allResults[j].Datasource
		}
		return left.After(right)
	})

	pageResults := paginateResults(allResults, page, pageSize)
	response := model.SearchResponse{
		Results:  pageResults,
		Sources:  sourceStatuses,
		Partial:  partial,
		CacheHit: false,
		TookMS:   time.Since(startedAt).Milliseconds(),
	}

	if req.UseCache {
		_ = s.cache.Set(ctx, cache.KindQuery, cacheKey, response, s.cache.QueryTTL())
	}

	return response, nil
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

	page := req.Page
	if page <= 0 {
		page = 1
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 200
	}
	if pageSize > 1000 {
		pageSize = 1000
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

func paginateResults(results []model.SearchResult, page, pageSize int) []model.SearchResult {
	start := (page - 1) * pageSize
	if start >= len(results) {
		return []model.SearchResult{}
	}
	end := start + pageSize
	if end > len(results) {
		end = len(results)
	}
	return results[start:end]
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
