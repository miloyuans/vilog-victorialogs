package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	"vilog-victorialogs/internal/util"
)

const (
	defaultJobResultsPageSize = 500
	maxJobResultsPageSize     = 1000
	defaultJobWorkerCount     = 10
	defaultJobBatchRows       = 25
)

type JobService struct {
	search *Service
	cfg    config.QueryJobsConfig
	logger *zap.Logger

	subMu       sync.Mutex
	subscribers map[string]map[chan model.QueryJobEvent]struct{}

	previewMu sync.RWMutex
	previews  map[string]*jobPreviewState

	activeMu sync.Mutex
	active   *activeQueryRun
}

type activeQueryRun struct {
	jobID  string
	cancel context.CancelFunc
}

type jobPreviewState struct {
	mu             sync.RWMutex
	perBucketLimit int
	rows           []model.SearchResult
	bucketCounts   map[string]int
}

type queryBucket struct {
	datasource  model.Datasource
	snapshot    model.DatasourceTagSnapshot
	serviceName string
}

type datasourceRunMeta struct {
	totalBuckets    int
	finishedBuckets int
	failedBuckets   int
	started         bool
}

func NewJobService(search *Service, cfg config.QueryJobsConfig, logger *zap.Logger) *JobService {
	if search == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &JobService{
		search:      search,
		cfg:         cfg,
		logger:      logger,
		subscribers: make(map[string]map[chan model.QueryJobEvent]struct{}),
		previews:    make(map[string]*jobPreviewState),
	}
}

func (s *JobService) Create(ctx context.Context, req model.SearchRequest) (model.QueryJobCreateResponse, error) {
	normalized, start, end, _, pageSize, err := normalizeRequest(req)
	if err != nil {
		return model.QueryJobCreateResponse{}, err
	}
	normalized, start, end = freezeQueryWindow(normalized, start, end)
	normalized.Page = 1
	normalized.PageSize = pageSize
	normalized.UseCache = false

	datasources, err := s.search.resolveDatasources(ctx, normalized.DatasourceIDs)
	if err != nil {
		return model.QueryJobCreateResponse{}, err
	}
	if len(datasources) == 0 {
		return model.QueryJobCreateResponse{}, fmt.Errorf("no datasource selected")
	}

	now := time.Now().UTC()
	job := model.QueryJob{
		ID:         util.NewPrefixedID("job"),
		Request:    normalized,
		Status:     model.QueryJobPending,
		StartedAt:  now,
		ExpiresAt:  now.Add(time.Duration(maxSearchInt(s.cfg.TTLHours, 1)) * time.Hour),
		Progress: model.QueryJobProgress{
			DatasourceTotal: len(datasources),
		},
		SourceStates: make([]model.QuerySourceState, 0, len(datasources)),
	}
	for _, datasource := range datasources {
		job.SourceStates = append(job.SourceStates, model.QuerySourceState{
			DatasourceID:   datasource.ID,
			DatasourceName: datasource.Name,
			Status:         string(model.QueryJobPending),
		})
	}

	if err := s.search.store.CreateQueryJob(ctx, job); err != nil {
		return model.QueryJobCreateResponse{}, err
	}
	s.initPreviewState(job.ID, normalized.PageSize)

	runCtx, cancel := context.WithCancel(context.Background())
	if previous := s.replaceActiveRun(job.ID, cancel); previous != nil {
		previous()
	}

	s.logger.Info("stream query job created",
		zap.String("job_id", job.ID),
		zap.Int("datasource_count", len(datasources)),
		zap.Int("service_count", len(normalized.ServiceNames)),
		zap.Time("start", start),
		zap.Time("end", end),
		zap.Int("per_bucket_limit", normalized.PageSize),
	)

	go s.run(runCtx, job.ID, normalized, start, end, datasources)

	return model.QueryJobCreateResponse{
		JobID:  job.ID,
		Status: job.Status,
	}, nil
}

func (s *JobService) Get(ctx context.Context, jobID string) (model.QueryJob, error) {
	return s.search.store.GetQueryJob(ctx, strings.TrimSpace(jobID))
}

func (s *JobService) Results(ctx context.Context, jobID, cursor string, pageSize int) (model.JobResultsPage, error) {
	return s.resultsPage(ctx, jobID, cursor, pageSize)
}

func (s *JobService) AllResults(ctx context.Context, jobID, cursor string, pageSize int) (model.JobResultsPage, error) {
	return s.resultsPage(ctx, jobID, cursor, pageSize)
}

func (s *JobService) resultsPage(ctx context.Context, jobID, cursor string, pageSize int) (model.JobResultsPage, error) {
	jobID = strings.TrimSpace(jobID)
	job, err := s.search.store.GetQueryJob(ctx, jobID)
	if err != nil {
		return model.JobResultsPage{}, err
	}

	currentCursor, err := decodeJobCursor(cursor)
	if err != nil {
		return model.JobResultsPage{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if currentCursor.JobID != "" && currentCursor.JobID != jobID {
		return model.JobResultsPage{}, fmt.Errorf("cursor does not belong to this job")
	}

	results := s.previewRows(jobID)
	limit := clampJobResultsPageSize(pageSize)
	startOffset := currentCursor.OffsetInSeg
	if startOffset < 0 {
		startOffset = 0
	}
	startIndex := int(startOffset)
	if startIndex > len(results) {
		startIndex = len(results)
	}
	endIndex := startIndex + limit
	if endIndex > len(results) {
		endIndex = len(results)
	}

	pageResults := append([]model.SearchResult(nil), results[startIndex:endIndex]...)
	hasMore := endIndex < len(results)
	nextCursor := ""
	if hasMore {
		nextCursor = encodeJobCursor(model.QueryResultsCursor{
			JobID:          jobID,
			OffsetInSeg:    int64(endIndex),
			FilterRevision: job.FilterRevision,
			Direction:      "forward",
		})
	}

	completed := isTerminalJobStatus(job.Status)
	partial := job.Status == model.QueryJobPartial || job.Status == model.QueryJobFailed

	return model.JobResultsPage{
		JobID:             job.ID,
		Status:            job.Status,
		Results:           compactSearchResultsForResponse(pageResults),
		Sources:           jobSourceStatuses(job),
		NextCursor:        nextCursor,
		HasMore:           hasMore,
		Completed:         completed,
		Partial:           partial,
		MatchedTotalSoFar: job.Progress.RowsMatched,
	}, nil
}

func (s *JobService) Subscribe(jobID string) (<-chan model.QueryJobEvent, func()) {
	ch := make(chan model.QueryJobEvent, 64)
	jobID = strings.TrimSpace(jobID)

	s.subMu.Lock()
	if _, ok := s.subscribers[jobID]; !ok {
		s.subscribers[jobID] = make(map[chan model.QueryJobEvent]struct{})
	}
	s.subscribers[jobID][ch] = struct{}{}
	s.subMu.Unlock()

	cancel := func() {
		s.subMu.Lock()
		defer s.subMu.Unlock()
		if listeners, ok := s.subscribers[jobID]; ok {
			delete(listeners, ch)
			if len(listeners) == 0 {
				delete(s.subscribers, jobID)
			}
		}
		close(ch)
	}

	return ch, cancel
}

func (s *JobService) StartCleanup(ctx context.Context) {
	if s == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			if err := s.cleanupExpired(ctx); err != nil {
				s.logger.Warn("cleanup expired query jobs failed", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *JobService) cleanupExpired(ctx context.Context) error {
	jobs, err := s.search.store.ListExpiredQueryJobs(ctx, time.Now().UTC(), 200)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		s.deletePreviewState(job.ID)
		if err := s.search.store.DeleteQueryJob(ctx, job.ID); err != nil {
			s.logger.Warn("delete expired query job failed", zap.String("job_id", job.ID), zap.Error(err))
		}
	}
	return nil
}

func (s *JobService) run(ctx context.Context, jobID string, req model.SearchRequest, start, end time.Time, datasources []model.Datasource) {
	defer s.clearActiveRun(jobID)

	storeCtx := context.Background()
	job, err := s.search.store.GetQueryJob(storeCtx, jobID)
	if err != nil {
		return
	}

	tagDefinitions, err := s.search.store.ListTagDefinitions(storeCtx)
	if err != nil {
		s.failJob(storeCtx, jobID, fmt.Errorf("load tag definitions: %w", err))
		return
	}

	buckets, datasourceMeta, err := s.buildJobBuckets(storeCtx, datasources, req.ServiceNames)
	if err != nil {
		s.failJob(storeCtx, jobID, err)
		return
	}

	job.Status = model.QueryJobRunning
	job.ResultVersion++
	if err := s.search.store.UpdateQueryJob(storeCtx, job); err != nil {
		s.logger.Warn("mark query job running failed", zap.String("job_id", jobID), zap.Error(err))
	}
	s.publish(jobID, model.QueryJobEvent{
		Type:     "status",
		JobID:    jobID,
		Status:   job.Status,
		Progress: job.Progress,
		Sources:  jobSourceStatuses(job),
	})

	workerCount := s.cfg.MaxConcurrentSourcesPerJob
	if workerCount <= 0 {
		workerCount = defaultJobWorkerCount
	}

	var (
		jobMu       sync.Mutex
		persistMu   sync.Mutex
		sequence    int64
		bucketErrs  int64
		lastPersist int64
		workers     sync.WaitGroup
		bucketQueue = make(chan queryBucket)
	)

	persistJob := func(snapshot model.QueryJob) {
		persistMu.Lock()
		defer persistMu.Unlock()
		if snapshot.ResultVersion < lastPersist {
			return
		}
		if err := s.search.store.UpdateQueryJob(storeCtx, snapshot); err != nil {
			s.logger.Warn("persist query job update failed", zap.String("job_id", snapshot.ID), zap.Error(err))
			return
		}
		lastPersist = snapshot.ResultVersion
	}

	snapshotJobLocked := func() (model.QueryJob, []model.QuerySourceStatus) {
		job.ResultVersion++
		jobCopy := cloneQueryJob(job)
		return jobCopy, jobSourceStatuses(jobCopy)
	}

	markDatasourceRunning := func(datasource model.Datasource) {
		jobMu.Lock()
		meta := datasourceMeta[datasource.ID]
		if meta == nil || meta.started {
			jobMu.Unlock()
			return
		}
		meta.started = true
		now := time.Now().UTC()
		s.updateSourceStateLocked(&job, datasource.ID, func(state *model.QuerySourceState) {
			state.Status = "running"
			state.StartedAt = &now
		})
		jobCopy, sources := snapshotJobLocked()
		jobMu.Unlock()
		persistJob(jobCopy)
		s.publish(jobID, model.QueryJobEvent{
			Type:         "progress",
			JobID:        jobID,
			Status:       jobCopy.Status,
			Sequence:     atomic.AddInt64(&sequence, 1),
			DatasourceID: datasource.ID,
			Progress:     jobCopy.Progress,
			Sources:      sources,
		})
	}

	recordBatch := func(datasource model.Datasource, rows []model.SearchResult) error {
		if len(rows) == 0 {
			return nil
		}
		accepted := s.appendPreviewRows(jobID, rows)
		if len(accepted) == 0 {
			return nil
		}

		jobMu.Lock()
		job.Progress.RowsWritten += int64(len(accepted))
		job.Progress.RowsMatched += int64(len(accepted))
		job.Totals.RowsTotal = job.Progress.RowsMatched
		job.Totals.RowsMatched = job.Progress.RowsMatched
		s.updateSourceStateLocked(&job, datasource.ID, func(state *model.QuerySourceState) {
			state.RowsFetched += int64(len(accepted))
			state.RowsMatched += int64(len(accepted))
		})
		jobCopy, sources := snapshotJobLocked()
		jobMu.Unlock()

		persistJob(jobCopy)
		s.publish(jobID, model.QueryJobEvent{
			Type:         "rows",
			JobID:        jobID,
			Status:       jobCopy.Status,
			Sequence:     atomic.AddInt64(&sequence, 1),
			RowCount:     int64(len(accepted)),
			DatasourceID: datasource.ID,
			Progress:     jobCopy.Progress,
			Sources:      sources,
			Rows:         compactSearchResultsForResponse(accepted),
		})
		return nil
	}

	finishDatasourceBucket := func(datasource model.Datasource, bucketErr error) {
		jobMu.Lock()
		meta := datasourceMeta[datasource.ID]
		if meta == nil {
			jobMu.Unlock()
			return
		}
		meta.finishedBuckets++
		if bucketErr != nil && !errors.Is(bucketErr, context.Canceled) {
			meta.failedBuckets++
			job.LastError = bucketErr.Error()
			s.updateSourceStateLocked(&job, datasource.ID, func(state *model.QuerySourceState) {
				state.Partial = true
				if state.Error == "" {
					state.Error = bucketErr.Error()
				}
			})
		}
		if meta.finishedBuckets >= meta.totalBuckets {
			now := time.Now().UTC()
			s.updateSourceStateLocked(&job, datasource.ID, func(state *model.QuerySourceState) {
				if meta.failedBuckets >= meta.totalBuckets && state.RowsMatched == 0 {
					state.Status = "error"
					state.Partial = true
				} else if meta.failedBuckets > 0 {
					state.Status = "partial"
					state.Partial = true
				} else {
					state.Status = "completed"
				}
				state.FinishedAt = &now
			})
		}
		jobCopy, sources := snapshotJobLocked()
		jobMu.Unlock()

		persistJob(jobCopy)
		s.publish(jobID, model.QueryJobEvent{
			Type:         "progress",
			JobID:        jobID,
			Status:       jobCopy.Status,
			Sequence:     atomic.AddInt64(&sequence, 1),
			DatasourceID: datasource.ID,
			Progress:     jobCopy.Progress,
			Sources:      sources,
			LastError:    firstNonEmpty(jobCopy.LastError, errorString(bucketErr)),
		})
	}

	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for bucket := range bucketQueue {
				if ctx.Err() != nil {
					return
				}
				markDatasourceRunning(bucket.datasource)
				err := s.runBucket(ctx, jobID, req, bucket, tagDefinitions, start, end, recordBatch)
				if err != nil && !errors.Is(err, context.Canceled) {
					atomic.AddInt64(&bucketErrs, 1)
					s.logger.Warn("stream query bucket failed",
						zap.String("job_id", jobID),
						zap.String("datasource", bucket.datasource.Name),
						zap.String("service", bucket.serviceName),
						zap.Error(err),
					)
				}
				finishDatasourceBucket(bucket.datasource, err)
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}

enqueueLoop:
	for _, bucket := range buckets {
		select {
		case <-ctx.Done():
			break enqueueLoop
		case bucketQueue <- bucket:
		}
	}
	close(bucketQueue)
	workers.Wait()

	jobMu.Lock()
	now := time.Now().UTC()
	job.FinishedAt = &now
	if errors.Is(ctx.Err(), context.Canceled) {
		job.Status = model.QueryJobCancelled
		for index := range job.SourceStates {
			if job.SourceStates[index].FinishedAt == nil {
				job.SourceStates[index].FinishedAt = &now
			}
			if job.SourceStates[index].Status == "" || job.SourceStates[index].Status == string(model.QueryJobPending) || job.SourceStates[index].Status == "running" {
				job.SourceStates[index].Status = string(model.QueryJobCancelled)
			}
		}
	} else if bucketErrs > 0 {
		if job.Progress.RowsMatched > 0 {
			job.Status = model.QueryJobPartial
		} else {
			job.Status = model.QueryJobFailed
			if job.LastError == "" {
				job.LastError = "all datasource/service buckets failed"
			}
		}
	} else {
		job.Status = model.QueryJobCompleted
	}
	recomputeJobSourceProgress(&job)
	finalJob, sources := snapshotJobLocked()
	jobMu.Unlock()

	persistJob(finalJob)

	eventType := "completed"
	switch finalJob.Status {
	case model.QueryJobCancelled:
		eventType = "cancelled"
	case model.QueryJobFailed:
		eventType = "failed"
	case model.QueryJobPartial:
		eventType = "partial"
	}
	s.publish(jobID, model.QueryJobEvent{
		Type:      eventType,
		JobID:     jobID,
		Status:    finalJob.Status,
		Sequence:  atomic.AddInt64(&sequence, 1),
		Progress:  finalJob.Progress,
		Sources:   sources,
		LastError: finalJob.LastError,
	})
	s.logger.Info("stream query job finished",
		zap.String("job_id", jobID),
		zap.String("status", string(finalJob.Status)),
		zap.Int64("rows_visible", finalJob.Progress.RowsMatched),
	)
}

func (s *JobService) runBucket(
	ctx context.Context,
	jobID string,
	req model.SearchRequest,
	bucket queryBucket,
	tagDefinitions []model.TagDefinition,
	start, end time.Time,
	recordBatch func(datasource model.Datasource, rows []model.SearchResult) error,
) error {
	serviceFilter := []string(nil)
	if strings.TrimSpace(bucket.serviceName) != "" {
		serviceFilter = []string{bucket.serviceName}
	}
	logsql := buildLogsQL(
		bucket.datasource,
		bucket.snapshot,
		tagDefinitions,
		req.Keyword,
		req.KeywordMode,
		serviceFilter,
		req.Tags,
	)

	batch := make([]model.SearchResult, 0, defaultJobBatchRows)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		rows := append([]model.SearchResult(nil), batch...)
		batch = batch[:0]
		return recordBatch(bucket.datasource, rows)
	}

	err := s.search.client.QueryStream(ctx, bucket.datasource, victorialogs.QueryChunkRequest{
		Query:  logsql,
		Start:  start,
		End:    end,
		Limit:  req.PageSize,
		Offset: 0,
	}, func(raw map[string]any) error {
		row := compactSearchResult(normalizeRow(bucket.datasource, bucket.snapshot, tagDefinitions, raw))
		batch = append(batch, row)
		if len(batch) >= defaultJobBatchRows {
			return flush()
		}
		return nil
	})
	if err != nil {
		if flushErr := flush(); flushErr != nil && !errors.Is(flushErr, context.Canceled) {
			s.logger.Warn("flush query batch after stream failure failed",
				zap.String("job_id", jobID),
				zap.String("datasource", bucket.datasource.Name),
				zap.String("service", bucket.serviceName),
				zap.Error(flushErr),
			)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		return err
	}
	if err := flush(); err != nil {
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		return err
	}
	return ctx.Err()
}

func (s *JobService) buildJobBuckets(ctx context.Context, datasources []model.Datasource, requestedServices []string) ([]queryBucket, map[string]*datasourceRunMeta, error) {
	buckets := make([]queryBucket, 0, len(datasources))
	meta := make(map[string]*datasourceRunMeta, len(datasources))

	for _, datasource := range datasources {
		snapshot, _ := s.search.store.GetSnapshot(ctx, datasource.ID)
		serviceNames, _, err := s.resolveJobServiceTargets(ctx, datasource.ID, requestedServices)
		if err != nil {
			return nil, nil, err
		}
		if len(serviceNames) == 0 {
			serviceNames = []string{""}
		}
		sort.Strings(serviceNames)
		meta[datasource.ID] = &datasourceRunMeta{totalBuckets: len(serviceNames)}
		for _, serviceName := range serviceNames {
			buckets = append(buckets, queryBucket{
				datasource:  datasource,
				snapshot:    snapshot,
				serviceName: strings.TrimSpace(serviceName),
			})
		}
	}

	sort.SliceStable(buckets, func(left, right int) bool {
		if buckets[left].datasource.Name == buckets[right].datasource.Name {
			return buckets[left].serviceName < buckets[right].serviceName
		}
		return buckets[left].datasource.Name < buckets[right].datasource.Name
	})
	return buckets, meta, nil
}

func (s *JobService) resolveJobServiceTargets(ctx context.Context, datasourceID string, requested []string) ([]string, bool, error) {
	if items := uniqueStrings(requested); len(items) > 0 {
		sort.Strings(items)
		return items, false, nil
	}

	entries, err := s.search.store.ListServiceCatalog(ctx, datasourceID)
	if err != nil {
		return []string{""}, false, nil
	}

	items := uniqueServiceNames(entries)
	if len(items) == 0 {
		return []string{""}, false, nil
	}
	sort.Strings(items)
	return items, true, nil
}

func (s *JobService) failJob(ctx context.Context, jobID string, err error) {
	job, loadErr := s.search.store.GetQueryJob(ctx, jobID)
	if loadErr != nil {
		return
	}
	now := time.Now().UTC()
	job.Status = model.QueryJobFailed
	job.LastError = err.Error()
	job.FinishedAt = &now
	for index := range job.SourceStates {
		if job.SourceStates[index].FinishedAt == nil {
			job.SourceStates[index].FinishedAt = &now
		}
		if strings.TrimSpace(job.SourceStates[index].Status) == "" || job.SourceStates[index].Status == string(model.QueryJobPending) || job.SourceStates[index].Status == "running" {
			job.SourceStates[index].Status = "error"
		}
		if strings.TrimSpace(job.SourceStates[index].Error) == "" {
			job.SourceStates[index].Error = err.Error()
		}
	}
	recomputeJobSourceProgress(&job)
	job.ResultVersion++
	if updateErr := s.search.store.UpdateQueryJob(ctx, job); updateErr != nil {
		s.logger.Warn("mark query job failed error", zap.String("job_id", jobID), zap.Error(updateErr))
	}
	s.publish(jobID, model.QueryJobEvent{
		Type:      "failed",
		JobID:     jobID,
		Status:    model.QueryJobFailed,
		LastError: err.Error(),
		Progress:  job.Progress,
		Sources:   jobSourceStatuses(job),
	})
}

func (s *JobService) publish(jobID string, event model.QueryJobEvent) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for ch := range s.subscribers[jobID] {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s *JobService) replaceActiveRun(jobID string, cancel context.CancelFunc) context.CancelFunc {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	var previous context.CancelFunc
	if s.active != nil {
		previous = s.active.cancel
	}
	s.active = &activeQueryRun{
		jobID:  strings.TrimSpace(jobID),
		cancel: cancel,
	}
	return previous
}

func (s *JobService) clearActiveRun(jobID string) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.active != nil && s.active.jobID == strings.TrimSpace(jobID) {
		s.active = nil
	}
}

func (s *JobService) updateSourceStateLocked(job *model.QueryJob, datasourceID string, mutate func(*model.QuerySourceState)) {
	if job == nil || mutate == nil {
		return
	}

	targetID := strings.TrimSpace(datasourceID)
	for index := range job.SourceStates {
		if strings.TrimSpace(job.SourceStates[index].DatasourceID) != targetID {
			continue
		}
		mutate(&job.SourceStates[index])
		recomputeJobSourceProgress(job)
		return
	}
}

func jobSourceStatuses(job model.QueryJob) []model.QuerySourceStatus {
	out := make([]model.QuerySourceStatus, 0, len(job.SourceStates))
	for _, state := range job.SourceStates {
		status := state.Status
		if status == "" {
			status = string(model.QueryJobPending)
		}
		out = append(out, model.QuerySourceStatus{
			Datasource: firstNonEmpty(state.DatasourceName, state.DatasourceID),
			Status:     status,
			Hits:       int(state.RowsMatched),
			Error:      state.Error,
		})
	}
	return out
}

func clampJobResultsPageSize(pageSize int) int {
	if pageSize <= 0 {
		return defaultJobResultsPageSize
	}
	if pageSize > maxJobResultsPageSize {
		return maxJobResultsPageSize
	}
	if pageSize < 50 {
		return 50
	}
	return pageSize
}

func encodeJobCursor(cursor model.QueryResultsCursor) string {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeJobCursor(raw string) (model.QueryResultsCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return model.QueryResultsCursor{}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return model.QueryResultsCursor{}, err
	}
	var cursor model.QueryResultsCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return model.QueryResultsCursor{}, err
	}
	return cursor, nil
}

func cloneQueryJob(job model.QueryJob) model.QueryJob {
	cloned := job
	cloned.SourceStates = append([]model.QuerySourceState(nil), job.SourceStates...)
	return cloned
}

func recomputeJobSourceProgress(job *model.QueryJob) {
	if job == nil {
		return
	}
	job.Progress.DatasourceTotal = len(job.SourceStates)
	job.Progress.DatasourceRunning = 0
	job.Progress.DatasourceCompleted = 0
	job.Progress.DatasourceFailed = 0
	for _, state := range job.SourceStates {
		switch state.Status {
		case "running":
			job.Progress.DatasourceRunning++
		case "completed", "partial":
			job.Progress.DatasourceCompleted++
		case "error":
			job.Progress.DatasourceFailed++
		}
	}
}

func (s *JobService) initPreviewState(jobID string, perBucketLimit int) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	if perBucketLimit <= 0 {
		perBucketLimit = 500
	}
	s.previewMu.Lock()
	s.previews[jobID] = &jobPreviewState{
		perBucketLimit: perBucketLimit,
		rows:           make([]model.SearchResult, 0, perBucketLimit),
		bucketCounts:   make(map[string]int),
	}
	s.previewMu.Unlock()
}

func (s *JobService) deletePreviewState(jobID string) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	s.previewMu.Lock()
	delete(s.previews, jobID)
	s.previewMu.Unlock()
}

func (s *JobService) previewState(jobID string) (*jobPreviewState, bool) {
	s.previewMu.RLock()
	state, ok := s.previews[strings.TrimSpace(jobID)]
	s.previewMu.RUnlock()
	return state, ok
}

func (s *JobService) appendPreviewRows(jobID string, rows []model.SearchResult) []model.SearchResult {
	state, ok := s.previewState(jobID)
	if !ok || state == nil || len(rows) == 0 {
		return nil
	}

	accepted := make([]model.SearchResult, 0, len(rows))
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, row := range rows {
		key := bucketPreviewKey(row)
		if state.bucketCounts[key] >= state.perBucketLimit {
			continue
		}
		state.bucketCounts[key]++
		state.rows = append(state.rows, row)
		accepted = append(accepted, row)
	}
	return accepted
}

func (s *JobService) previewRows(jobID string) []model.SearchResult {
	state, ok := s.previewState(jobID)
	if !ok || state == nil {
		return nil
	}

	state.mu.RLock()
	rows := append([]model.SearchResult(nil), state.rows...)
	state.mu.RUnlock()
	sortPreviewRows(rows)
	return rows
}

func bucketPreviewKey(row model.SearchResult) string {
	return strings.TrimSpace(row.Datasource) + "\x00" + firstNonEmpty(strings.TrimSpace(row.Service), "__all__")
}

func sortPreviewRows(rows []model.SearchResult) {
	sort.SliceStable(rows, func(left, right int) bool {
		if rows[left].Timestamp == rows[right].Timestamp {
			if rows[left].Datasource == rows[right].Datasource {
				if rows[left].Service == rows[right].Service {
					return rows[left].Message > rows[right].Message
				}
				return rows[left].Service < rows[right].Service
			}
			return rows[left].Datasource < rows[right].Datasource
		}
		return rows[left].Timestamp > rows[right].Timestamp
	})
}

func compactSearchResult(item model.SearchResult) model.SearchResult {
	clone := item
	clone.SearchText = ""
	clone.Raw = nil
	return clone
}

func freezeQueryWindow(req model.SearchRequest, start, end time.Time) (model.SearchRequest, time.Time, time.Time) {
	start = start.UTC().Truncate(time.Minute)
	end = end.UTC().Truncate(time.Minute)
	if !end.After(start) {
		end = start.Add(time.Minute)
	}
	req.Start = start.Format(time.RFC3339)
	req.End = end.Format(time.RFC3339)
	return req, start, end
}

func isTerminalJobStatus(status model.QueryJobStatus) bool {
	switch status {
	case model.QueryJobCompleted, model.QueryJobFailed, model.QueryJobPartial, model.QueryJobCancelled:
		return true
	default:
		return false
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
