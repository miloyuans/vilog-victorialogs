package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	localstore "vilog-victorialogs/internal/store/local"
	"vilog-victorialogs/internal/util"
)

const (
	defaultJobResultsPageSize = 100
	maxJobResultsPageSize     = 200
	defaultJobFlushRows       = 200
)

type JobService struct {
	search   *Service
	cfg      config.QueryJobsConfig
	logger   *zap.Logger
	segments *localstore.QuerySegmentStore
	sem      chan struct{}

	subMu        sync.Mutex
	subscribers  map[string]map[chan model.QueryJobEvent]struct{}
}

func NewJobService(search *Service, cfg config.QueryJobsConfig, logger *zap.Logger) *JobService {
	if search == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	maxJobs := cfg.MaxConcurrentJobs
	if maxJobs <= 0 {
		maxJobs = 4
	}
	return &JobService{
		search:      search,
		cfg:         cfg,
		logger:      logger,
		segments:    localstore.NewQuerySegmentStore(cfg.BaseDir),
		sem:         make(chan struct{}, maxJobs),
		subscribers: make(map[string]map[chan model.QueryJobEvent]struct{}),
	}
}

func (s *JobService) Create(ctx context.Context, req model.SearchRequest) (model.QueryJobCreateResponse, error) {
	normalized, start, end, _, pageSize, err := normalizeRequest(req)
	if err != nil {
		return model.QueryJobCreateResponse{}, err
	}
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
	s.logger.Info("query job created",
		zap.String("job_id", job.ID),
		zap.Int("datasource_count", len(datasources)),
		zap.Int("service_count", len(normalized.ServiceNames)),
		zap.Time("start", start),
		zap.Time("end", end),
	)

	go s.run(job.ID, normalized, start, end, datasources)

	return model.QueryJobCreateResponse{
		JobID:  job.ID,
		Status: job.Status,
	}, nil
}

func (s *JobService) Get(ctx context.Context, jobID string) (model.QueryJob, error) {
	return s.search.store.GetQueryJob(ctx, strings.TrimSpace(jobID))
}

func (s *JobService) Results(ctx context.Context, jobID, cursor string, pageSize int) (model.JobResultsPage, error) {
	jobID = strings.TrimSpace(jobID)
	job, err := s.search.store.GetQueryJob(ctx, jobID)
	if err != nil {
		return model.JobResultsPage{}, err
	}
	segments, err := s.search.store.ListQuerySegments(ctx, jobID)
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

	limit := clampJobResultsPageSize(pageSize)
	results := make([]model.SearchResult, 0, limit)
	nextCursor := ""
	hasStoredMore := false
	startSeq := currentCursor.SegmentSeq
	startOffset := currentCursor.OffsetInSeg

	for index, segment := range segments {
		if segment.Sequence < startSeq {
			continue
		}
		offset := int64(0)
		if segment.Sequence == startSeq {
			offset = startOffset
		}
		rowIndex := int64(0)
		stop := false
		if err := s.segments.ReadRows(segment.FilePath, func(row model.SearchResult) (bool, error) {
			if rowIndex < offset {
				rowIndex++
				return true, nil
			}
			results = append(results, row)
			rowIndex++
			if len(results) >= limit {
				hasMoreInCurrent := rowIndex < segment.RowCount
				hasLaterSegments := index < len(segments)-1
				hasStoredMore = hasMoreInCurrent || hasLaterSegments
				if hasStoredMore {
					nextCursor = encodeJobCursor(model.QueryResultsCursor{
						JobID:          jobID,
						SegmentSeq:     segment.Sequence,
						OffsetInSeg:    rowIndex,
						FilterRevision: job.FilterRevision,
						Direction:      "forward",
					})
				}
				stop = true
				return false, nil
			}
			return true, nil
		}); err != nil {
			return model.JobResultsPage{}, err
		}
		if stop {
			break
		}
	}

	completed := job.Status == model.QueryJobCompleted || job.Status == model.QueryJobPartial || job.Status == model.QueryJobFailed || job.Status == model.QueryJobCancelled
	partial := job.Status == model.QueryJobPartial || job.Status == model.QueryJobFailed

	return model.JobResultsPage{
		JobID:             job.ID,
		Status:            job.Status,
		Results:           compactSearchResultsForResponse(results),
		Sources:           jobSourceStatuses(job),
		NextCursor:        nextCursor,
		HasMore:           hasStoredMore,
		Completed:         completed,
		Partial:           partial,
		MatchedTotalSoFar: job.Progress.RowsMatched,
	}, nil
}

func (s *JobService) Subscribe(jobID string) (<-chan model.QueryJobEvent, func()) {
	ch := make(chan model.QueryJobEvent, 32)
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
		_ = s.segments.DeleteJob(job.ID)
		if err := s.search.store.DeleteQueryJob(ctx, job.ID); err != nil {
			s.logger.Warn("delete expired query job failed", zap.String("job_id", job.ID), zap.Error(err))
		}
	}
	return nil
}

func (s *JobService) run(jobID string, req model.SearchRequest, start, end time.Time, datasources []model.Datasource) {
	s.sem <- struct{}{}
	defer func() {
		<-s.sem
	}()

	ctx := context.Background()
	tagDefinitions, err := s.search.store.ListTagDefinitions(ctx)
	if err != nil {
		s.failJob(ctx, jobID, fmt.Errorf("load tag definitions: %w", err))
		return
	}
	if _, err := s.segments.EnsureJobDir(jobID); err != nil {
		s.failJob(ctx, jobID, err)
		return
	}

	job, err := s.search.store.GetQueryJob(ctx, jobID)
	if err != nil {
		return
	}

	var (
		jobMu       sync.Mutex
		sequence    int64
		partialSeen bool
		errorSeen   bool
	)

	job.Status = model.QueryJobRunning
	job.Progress.DatasourceRunning = len(datasources)
	_ = s.search.store.UpdateQueryJob(ctx, job)
	s.publish(jobID, model.QueryJobEvent{
		Type:     "status",
		JobID:    jobID,
		Status:   job.Status,
		Progress: job.Progress,
	})

	sourceSemSize := s.cfg.MaxConcurrentSourcesPerJob
	if sourceSemSize <= 0 {
		sourceSemSize = 2
	}
	sourceSem := make(chan struct{}, sourceSemSize)
	var wg sync.WaitGroup

	for _, datasource := range datasources {
		ds := datasource
		wg.Add(1)
		go func() {
			defer wg.Done()
			sourceSem <- struct{}{}
			defer func() { <-sourceSem }()

			if err := s.updateSourceState(ctx, &jobMu, &job, ds.ID, func(state *model.QuerySourceState) {
				now := time.Now().UTC()
				state.Status = "running"
				state.StartedAt = &now
			}); err != nil {
				s.logger.Warn("mark query source running failed", zap.String("job_id", jobID), zap.String("datasource", ds.Name), zap.Error(err))
			}

			sourcePartial, sourceErr := s.runDatasource(ctx, jobID, &sequence, &jobMu, &job, req, ds, tagDefinitions, start, end)

			if sourceErr != nil {
				jobMu.Lock()
				errorSeen = true
				partialSeen = true
				jobMu.Unlock()
				_ = s.updateSourceState(ctx, &jobMu, &job, ds.ID, func(state *model.QuerySourceState) {
					now := time.Now().UTC()
					state.Status = "error"
					state.Error = sourceErr.Error()
					state.Partial = true
					state.FinishedAt = &now
				})
				return
			}

			if sourcePartial {
				jobMu.Lock()
				partialSeen = true
				jobMu.Unlock()
			}
			_ = s.updateSourceState(ctx, &jobMu, &job, ds.ID, func(state *model.QuerySourceState) {
				now := time.Now().UTC()
				state.Status = map[bool]string{true: "partial", false: "completed"}[sourcePartial]
				state.Partial = sourcePartial
				state.FinishedAt = &now
			})
		}()
	}

	wg.Wait()

	jobMu.Lock()
	now := time.Now().UTC()
	job.FinishedAt = &now
	job.Progress.DatasourceRunning = 0
	job.Progress.DatasourceCompleted = 0
	job.Progress.DatasourceFailed = 0
	for _, state := range job.SourceStates {
		switch state.Status {
		case "completed":
			job.Progress.DatasourceCompleted++
		case "partial":
			job.Progress.DatasourceCompleted++
		case "error":
			job.Progress.DatasourceFailed++
		}
	}
	switch {
	case errorSeen && job.Progress.RowsMatched == 0:
		job.Status = model.QueryJobFailed
	case partialSeen || errorSeen:
		job.Status = model.QueryJobPartial
	default:
		job.Status = model.QueryJobCompleted
	}
	finalJob := cloneQueryJob(job)
	jobMu.Unlock()

	if err := s.search.store.UpdateQueryJob(ctx, finalJob); err != nil {
		s.logger.Warn("finalize query job failed", zap.String("job_id", jobID), zap.Error(err))
	}

	eventType := "completed"
	if finalJob.Status == model.QueryJobFailed {
		eventType = "failed"
	} else if finalJob.Status == model.QueryJobPartial {
		eventType = "partial"
	}
	s.publish(jobID, model.QueryJobEvent{
		Type:      eventType,
		JobID:     jobID,
		Status:    finalJob.Status,
		Progress:  finalJob.Progress,
		LastError: finalJob.LastError,
	})
	s.logger.Info("query job finished",
		zap.String("job_id", jobID),
		zap.String("status", string(finalJob.Status)),
		zap.Int64("rows_matched", finalJob.Progress.RowsMatched),
		zap.Int64("segments_written", finalJob.Progress.SegmentsWritten),
	)
}

func (s *JobService) runDatasource(
	ctx context.Context,
	jobID string,
	sequence *int64,
	jobMu *sync.Mutex,
	job *model.QueryJob,
	req model.SearchRequest,
	datasource model.Datasource,
	tagDefinitions []model.TagDefinition,
	start, end time.Time,
) (bool, error) {
	snapshot, _ := s.search.store.GetSnapshot(ctx, datasource.ID)
	keywords := splitKeywords(req.Keyword)
	serviceNames := uniqueStrings(req.ServiceNames)
	if len(serviceNames) == 0 {
		serviceNames = []string{""}
	}

	chunkWindow := s.cfg.ChunkWindow
	if chunkWindow <= 0 {
		chunkWindow = 15 * time.Minute
	}
	sourceLimit := s.search.cfg.SourceRequestLimit
	if sourceLimit <= 0 {
		sourceLimit = 1000
	}
	if sourceLimit > 500 {
		sourceLimit = 500
	}

	flushLimit := s.cfg.SegmentMaxRows
	if flushLimit <= 0 {
		flushLimit = defaultJobFlushRows
	}
	if flushLimit > defaultJobFlushRows {
		flushLimit = defaultJobFlushRows
	}

	buffer := make([]model.SearchResult, 0, flushLimit)
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		seq := atomic.AddInt64(sequence, 1)
		meta, err := s.writeSegment(jobID, seq, buffer)
		if err != nil {
			return err
		}
		if err := s.search.store.CreateQuerySegment(ctx, model.QuerySegment{
			ID:            util.NewPrefixedID("seg"),
			JobID:         jobID,
			Sequence:      seq,
			FilePath:      meta.FilePath,
			RowCount:      meta.RowCount,
			SizeBytes:     meta.SizeBytes,
			TimeMin:       meta.TimeMin,
			TimeMax:       meta.TimeMax,
			DatasourceIDs: meta.DatasourceIDs,
			Completed:     true,
			CreatedAt:     time.Now().UTC(),
		}); err != nil {
			return err
		}
		jobMu.Lock()
		job.Progress.SegmentsWritten++
		job.Progress.BytesWritten += meta.SizeBytes
		s.updateSourceStateLocked(job, datasource.ID, func(state *model.QuerySourceState) {
			state.SegmentsWritten++
		})
		jobCopy := cloneQueryJob(*job)
		jobMu.Unlock()
		if err := s.search.store.UpdateQueryJob(ctx, jobCopy); err != nil {
			s.logger.Warn("update query job after segment flush failed", zap.String("job_id", jobID), zap.Error(err))
		}
		s.publish(jobID, model.QueryJobEvent{
			Type:         "segment_ready",
			JobID:        jobID,
			Sequence:     seq,
			RowCount:     meta.RowCount,
			DatasourceID: datasource.ID,
			Progress:     jobCopy.Progress,
		})
		buffer = buffer[:0]
		return nil
	}

	for _, serviceName := range serviceNames {
		logsql := buildSourceLogsQL(datasource, snapshot, serviceName)
		for windowStart := start.UTC(); windowStart.Before(end.UTC()); {
			windowEnd := windowStart.Add(chunkWindow)
			if windowEnd.After(end.UTC()) {
				windowEnd = end.UTC()
			}

			offset := 0
			for {
				batchCount := 0
				streamErr := s.search.client.QueryStream(ctx, datasource, victorialogs.QueryChunkRequest{
					Query:  logsql,
					Start:  windowStart,
					End:    windowEnd,
					Limit:  sourceLimit,
					Offset: offset,
				}, func(raw map[string]any) error {
					batchCount++
					normalized := normalizeRow(datasource, snapshot, tagDefinitions, raw)
					jobMu.Lock()
					job.Progress.RowsWritten++
					s.updateSourceStateLocked(job, datasource.ID, func(state *model.QuerySourceState) {
						state.RowsFetched++
					})
					jobMu.Unlock()
					if !matchesKeywordFilter(normalized, keywords, req.KeywordMode) {
						return nil
					}
					if !matchesTagFilters(normalized, req.Tags) {
						return nil
					}
					buffer = append(buffer, normalized)
					jobMu.Lock()
					job.Progress.RowsMatched++
					job.Totals.RowsMatched = job.Progress.RowsMatched
					job.Totals.RowsTotal = job.Progress.RowsWritten
					s.updateSourceStateLocked(job, datasource.ID, func(state *model.QuerySourceState) {
						state.RowsMatched++
					})
					jobMu.Unlock()
					if len(buffer) >= flushLimit {
						return flush()
					}
					return nil
				})
				if streamErr != nil {
					_ = flush()
					return true, streamErr
				}
				if batchCount < sourceLimit {
					break
				}
				offset += sourceLimit
			}

			if err := flush(); err != nil {
				return true, err
			}
			windowStart = windowEnd
		}
	}

	jobMu.Lock()
	jobCopy := cloneQueryJob(*job)
	jobMu.Unlock()
	if err := s.search.store.UpdateQueryJob(ctx, jobCopy); err != nil {
		s.logger.Warn("persist query job progress failed", zap.String("job_id", jobID), zap.Error(err))
	}
	s.publish(jobID, model.QueryJobEvent{
		Type:         "progress",
		JobID:        jobID,
		DatasourceID: datasource.ID,
		Progress:     jobCopy.Progress,
	})

	return false, nil
}

func (s *JobService) writeSegment(jobID string, sequence int64, rows []model.SearchResult) (localstore.SegmentMeta, error) {
	writer, err := s.segments.OpenWriter(jobID, sequence)
	if err != nil {
		return localstore.SegmentMeta{}, err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return localstore.SegmentMeta{}, err
		}
	}
	return writer.Close()
}

func (s *JobService) updateSourceState(ctx context.Context, jobMu *sync.Mutex, job *model.QueryJob, datasourceID string, mutate func(*model.QuerySourceState)) error {
	jobMu.Lock()
	s.updateSourceStateLocked(job, datasourceID, mutate)
	jobCopy := cloneQueryJob(*job)
	jobMu.Unlock()
	if err := s.search.store.UpdateQueryJob(ctx, jobCopy); err != nil {
		return err
	}
	s.publish(job.ID, model.QueryJobEvent{
		Type:         "progress",
		JobID:        job.ID,
		DatasourceID: datasourceID,
		Status:       jobCopy.Status,
		Progress:     jobCopy.Progress,
	})
	return nil
}

func (s *JobService) updateSourceStateLocked(job *model.QueryJob, datasourceID string, mutate func(*model.QuerySourceState)) {
	for index := range job.SourceStates {
		if job.SourceStates[index].DatasourceID == datasourceID {
			mutate(&job.SourceStates[index])
			recomputeJobSourceProgress(job)
			return
		}
	}
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
	if updateErr := s.search.store.UpdateQueryJob(ctx, job); updateErr != nil {
		s.logger.Warn("mark query job failed error", zap.String("job_id", jobID), zap.Error(updateErr))
	}
	s.publish(jobID, model.QueryJobEvent{
		Type:      "failed",
		JobID:     jobID,
		Status:    model.QueryJobFailed,
		LastError: err.Error(),
		Progress:  job.Progress,
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
	if pageSize < 25 {
		return 25
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
