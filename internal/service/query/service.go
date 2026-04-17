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

const (
	maxDatasourceWindow = 100000
	remoteChunkLimit    = 1000
)

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

	resultsCh := make(chan sourceResult, len(datasources))
	var wg sync.WaitGroup

	for _, datasource := range datasources {
		ds := datasource
		wg.Add(1)
		go func() {
			defer wg.Done()

			snapshot, _ := s.store.GetSnapshot(ctx, ds.ID)
			logsql := buildLogsQL(ds, snapshot, tagDefinitions, req.Keyword, req.ServiceNames, req.Tags)
			rows, truncated, queryErr := s.fetchDatasourceWindow(
				ctx,
				ds,
				snapshot,
				tagDefinitions,
				logsql,
				start,
				end,
				page*pageSize,
			)
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
						Status:     statusLabelForRows(truncated, true),
						Hits:       0,
					},
				}
				return
			}

			resultsCh <- sourceResult{
				results: rows,
				status: model.QuerySourceStatus{
					Datasource: ds.Name,
					Status:     statusLabelForRows(truncated, false),
					Hits:       len(rows),
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
		if item.status.Status == "error" || item.status.Status == "partial" {
			partial = true
		}
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

	pageResults := paginateResults(allResults, page, pageSize)
	response := model.SearchResponse{
		Results:  pageResults,
		Sources:  sourceStatuses,
		Total:    len(allResults),
		Partial:  partial,
		CacheHit: false,
		TookMS:   time.Since(startedAt).Milliseconds(),
	}

	if req.UseCache {
		_ = s.cache.Set(ctx, cache.KindQuery, cacheKey, response, s.cache.QueryTTL())
	}

	return response, nil
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
	if requestedWindow > maxDatasourceWindow {
		requestedWindow = maxDatasourceWindow
	}

	normalizedRows := make([]model.SearchResult, 0, requestedWindow)
	offset := 0
	truncated := false
	lastBatchSize := 0
	lastBatchLimit := 0

	for offset < requestedWindow {
		limit := remoteChunkLimit
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
	return strings.TrimSpace(row.Timestamp) == ""
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

func statusLabelForRows(truncated, empty bool) string {
	if truncated {
		return "partial"
	}
	if empty {
		return "empty"
	}
	return "ok"
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
