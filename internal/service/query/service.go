package query

import (
	"context"
	"encoding/json"
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
	"vilog-victorialogs/internal/util"
)

type Service struct {
	store  *mongostore.Store
	cache  *cache.Service
	client *victorialogs.Client
	cfg    config.CacheConfig
	logger *zap.Logger

	partitionSyncMu         sync.Mutex
	partitionSyncCond       *sync.Cond
	partitionSyncs          map[string]*partitionSyncTask
	interactivePendingSyncs []string
	maintenancePendingSyncs []string
	interactiveUrgentSem    chan struct{}
	servicePriorityMu       sync.Mutex
	servicePriorityTTL      map[string]time.Time
}

func New(store *mongostore.Store, cacheService *cache.Service, client *victorialogs.Client, cfg config.CacheConfig, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	maintenanceConcurrency := cfg.MaintenanceSyncConcurrency
	if maintenanceConcurrency <= 0 {
		maintenanceConcurrency = cfg.LocalLogCheckConcurrency
	}
	if maintenanceConcurrency <= 0 {
		maintenanceConcurrency = 4
	}
	interactiveConcurrency := cfg.InteractiveSyncConcurrency
	if interactiveConcurrency <= 0 {
		interactiveConcurrency = 2
	}
	svc := &Service{
		store:  store,
		cache:  cacheService,
		client: client,
		cfg:    cfg,
		logger: logger,
		partitionSyncs:       make(map[string]*partitionSyncTask),
		interactiveUrgentSem: make(chan struct{}, 1),
		servicePriorityTTL:   make(map[string]time.Time),
	}
	svc.partitionSyncCond = sync.NewCond(&svc.partitionSyncMu)
	svc.startPartitionWorkers(partitionSyncQueueInteractive, interactiveConcurrency)
	svc.startPartitionWorkers(partitionSyncQueueMaintenance, maintenanceConcurrency)
	return svc
}

func (s *Service) StartHotSync(ctx context.Context) {
	if s.cache == nil || strings.TrimSpace(s.cfg.LocalLogDir) == "" || s.cfg.LocalLogRefreshInterval <= 0 {
		return
	}

	s.logger.Info("starting tracked hot log cache sync",
		zap.String("dir", s.cfg.LocalLogDir),
		zap.Duration("interval", s.cfg.LocalLogRefreshInterval),
	)

	s.runHotSyncCycle(ctx)

	ticker := time.NewTicker(s.cfg.LocalLogRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("stopping tracked hot log cache sync")
			return
		case <-ticker.C:
			s.runHotSyncCycle(ctx)
		}
	}
}

func (s *Service) runHotSyncCycle(ctx context.Context) {
	if s.hasInteractiveSyncPressure() {
		s.logger.Debug("skip tracked hot log cache sync cycle because interactive syncs are active")
		return
	}
	if err := s.refreshTrackedHotPartitions(ctx); err != nil {
		s.logger.Warn("refresh tracked hot log partitions failed", zap.Error(err))
	}
}

func (s *Service) Search(ctx context.Context, req model.SearchRequest) (model.SearchResponse, error) {
	startedAt := time.Now().UTC()
	normalized, start, end, _, pageSize, err := normalizeRequest(req)
	if err != nil {
		return model.SearchResponse{}, err
	}

	datasources, err := s.resolveDatasources(ctx, normalized.DatasourceIDs)
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
		zap.Int("keyword_group_count", len(splitKeywords(normalized.Keyword))),
		zap.String("keyword_mode", normalized.KeywordMode),
		zap.Int("tag_filter_count", len(normalized.Tags)),
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
			rows, status, cacheHit, sourcePartial, queryErr := s.searchDatasource(ctx, ds, snapshot, tagDefinitions, normalized, start, end, now, pageSize)
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

	preFilterCount := len(allResults)
	allResults = applySearchFilters(allResults, normalized)
	s.logger.Debug("search filters applied",
		zap.Int("before", preFilterCount),
		zap.Int("after", len(allResults)),
	)

	sortSearchResults(allResults)
	allResults = limitResultsPerService(allResults, pageSize)

	totalMatches := len(allResults)
	responseLimit := responseVisibleLimit(pageSize, allResults)
	if len(allResults) > responseLimit {
		allResults = allResults[:responseLimit]
		partial = true
	}
	allResults = compactSearchResultsForResponse(allResults)
	response := model.SearchResponse{
		Results:  allResults,
		Sources:  sourceStatuses,
		Total:    totalMatches,
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

func compactSearchResultsForResponse(items []model.SearchResult) []model.SearchResult {
	if len(items) == 0 {
		return items
	}
	compacted := make([]model.SearchResult, 0, len(items))
	for _, item := range items {
		clone := item
		clone.Raw = nil
		compacted = append(compacted, clone)
	}
	return compacted
}

func (s *Service) refreshTrackedHotPartitions(ctx context.Context) error {
	metas, err := s.cache.ListTrackedLogPartitions()
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		s.logger.Debug("tracked hot log cache sync skipped: no tracked partitions")
		return nil
	}

	now := time.Now().UTC()
	tagDefinitions, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return err
	}

	datasourceCache := make(map[string]model.Datasource)
	snapshotCache := make(map[string]model.DatasourceTagSnapshot)
	refreshed := 0
	for _, meta := range metas {
		day, parseErr := time.Parse("2006-01-02", meta.Date)
		if parseErr != nil {
			continue
		}
		if !s.cache.LogPartitionNeedsRefresh(meta, day, now) {
			continue
		}

		datasource, ok := datasourceCache[meta.DatasourceID]
		if !ok {
			loaded, loadErr := s.store.GetDatasource(ctx, meta.DatasourceID)
			if loadErr != nil {
				s.logger.Warn("load datasource for tracked hot sync failed",
					zap.Error(loadErr),
					zap.String("datasource_id", meta.DatasourceID),
					zap.String("service", displayServiceName(meta.Service)),
					zap.String("date", meta.Date),
				)
				continue
			}
			if !loaded.Enabled {
				continue
			}
			datasource = loaded
			datasourceCache[meta.DatasourceID] = loaded
		}

		partition, hit, loadErr := s.cache.LoadLogPartition(day, meta.Service, datasource)
		if loadErr != nil || !hit {
			if loadErr != nil {
				s.logger.Warn("load tracked hot partition failed",
					zap.Error(loadErr),
					zap.String("datasource", datasource.Name),
					zap.String("service", displayServiceName(meta.Service)),
					zap.String("date", meta.Date),
				)
			}
			continue
		}

		snapshot, ok := snapshotCache[datasource.ID]
		if !ok {
			loaded, _ := s.store.GetSnapshot(ctx, datasource.ID)
			snapshot = loaded
			snapshotCache[datasource.ID] = loaded
		}

		queue := s.preferredSyncQueue(datasource, meta.Service, now, partitionSyncQueueMaintenance)
		task, _ := s.startPartitionSync(day, datasource, meta.Service, "tracked-refresh", queue, func(syncCtx context.Context) error {
			_, _, refreshErr := s.refreshPartitionIncremental(syncCtx, datasource, snapshot, tagDefinitions, meta.Service, day, time.Now().UTC(), partition)
			return refreshErr
		})
		if refreshErr := s.waitPartitionSync(ctx, task); refreshErr != nil {
			s.logger.Warn("tracked hot partition incremental refresh failed",
				zap.Error(refreshErr),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(meta.Service)),
				zap.String("date", meta.Date),
			)
			continue
		}
		refreshed++
	}

	s.logger.Debug("tracked hot log cache sync cycle completed",
		zap.Int("tracked_partitions", len(metas)),
		zap.Int("refreshed", refreshed),
	)
	return nil
}

func (s *Service) searchDatasource(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	req model.SearchRequest,
	start, end, now time.Time,
	pageSize int,
) ([]model.SearchResult, model.QuerySourceStatus, bool, bool, error) {
	services, err := s.resolveServiceTargets(ctx, datasource.ID, req.ServiceNames)
	if err != nil {
		return nil, model.QuerySourceStatus{}, false, true, err
	}
	explicitServiceSelection := len(req.ServiceNames) > 0
	for _, serviceName := range services {
		s.markServiceInteractive(datasource, serviceName, now)
	}

	rows := make([]model.SearchResult, 0)
	cacheHit := true
	partial := false
	previewLimit := 0
	if explicitServiceSelection {
		previewLimit = pageSize
		if previewLimit <= 0 {
			previewLimit = 200
		}
		if previewLimit > 1000 {
			previewLimit = 1000
		}
	} else {
		previewLimit = pageSize
		if previewLimit <= 0 {
			previewLimit = 200
		}
		if previewLimit < 100 {
			previewLimit = 100
		}
		if previewLimit > 300 {
			previewLimit = 300
		}
	}
	for _, serviceName := range services {
		serviceRows, serviceCacheHit, servicePartial, err := s.searchServiceWindow(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, now, req.UseCache, previewLimit)
		if err != nil {
			return nil, model.QuerySourceStatus{}, false, true, err
		}
		rows = append(rows, serviceRows...)
		cacheHit = cacheHit && serviceCacheHit
		partial = partial || servicePartial
	}

	if !explicitServiceSelection && (len(rows) == 0 || partial) {
		aggregatePreviewLimit := pageSize
		if aggregatePreviewLimit <= 0 {
			aggregatePreviewLimit = 200
		}
		if aggregatePreviewLimit < 100 {
			aggregatePreviewLimit = 100
		}
		if aggregatePreviewLimit > 300 {
			aggregatePreviewLimit = 300
		}
		previewRows, previewPartial, previewErr := s.fetchAllServicePreviewWindow(ctx, datasource, snapshot, tagDefinitions, start, end, aggregatePreviewLimit)
		if previewErr != nil {
			s.logger.Warn("prepare all-service preview failed",
				zap.Error(previewErr),
				zap.String("datasource", datasource.Name),
				zap.Time("start", start),
				zap.Time("end", end),
				zap.Int("limit", aggregatePreviewLimit),
			)
		} else if len(previewRows) > 0 {
			mergedRows, mergePartial := s.mergeSourceRowsLimited(rows, previewRows, datasource, "", cacheDay(start), "all-service-preview")
			sortSearchResults(mergedRows)
			rows = limitResultsPerService(mergedRows, aggregatePreviewLimit)
			cacheHit = false
			partial = partial || previewPartial || mergePartial
			s.logger.Info("prepared all-service preview rows",
				zap.String("datasource", datasource.Name),
				zap.Time("start", start),
				zap.Time("end", end),
				zap.Int("rows", len(previewRows)),
				zap.Int("limit", aggregatePreviewLimit),
			)
		}
	}

	rows = applySearchFilters(rows, req)

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
	previewLimit int,
) ([]model.SearchResult, bool, bool, error) {
	rows := make([]model.SearchResult, 0)
	cacheHit := true
	partial := false
	for day := cacheDay(start); !day.After(cacheDay(end)); day = day.Add(24 * time.Hour) {
		partition, partitionCacheHit, partitionPartial, err := s.loadPartitionWindow(ctx, datasource, snapshot, tagDefinitions, serviceName, day, start, end, now, useCache, previewLimit)
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
	day, windowStart, windowEnd, now time.Time,
	useCache bool,
	previewLimit int,
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
			if !s.cfg.BackgroundSyncEnabled {
				if previewLimit > 0 && (len(partition.Rows) == 0 || s.cache.LogPartitionNeedsRefresh(partition.Meta, day, now)) {
					previewPartition, previewPartial, previewErr := s.buildPreviewOnlyPartition(ctx, datasource, snapshot, tagDefinitions, serviceName, day, windowStart, windowEnd, now, previewLimit, true)
					if previewErr != nil {
						s.logger.Warn("refresh preview-only log partition failed",
							zap.Error(previewErr),
							zap.String("datasource", datasource.Name),
							zap.String("service", displayServiceName(serviceName)),
							zap.String("day", cacheDay(day).Format("2006-01-02")),
						)
					} else if len(previewPartition.Rows) > 0 || len(partition.Rows) == 0 {
						s.logger.Info("refreshed preview-only log partition",
							zap.String("datasource", datasource.Name),
							zap.String("service", displayServiceName(serviceName)),
							zap.String("day", cacheDay(day).Format("2006-01-02")),
							zap.Int("rows", len(previewPartition.Rows)),
						)
						return previewPartition, false, previewPartial, nil
					}
				}
				return partition, true, true, nil
			}
			if partition.Meta.Building {
				previewRefreshed := false
				if previewLimit > 0 && len(partition.Rows) == 0 && s.shouldRefreshBuildingPreview(partition.Meta, now) {
					previewStart, previewEnd := intersectWindowWithDay(day, windowStart, windowEnd, now)
					if previewEnd.After(previewStart) {
						previewRows, previewErr := s.fetchPreviewSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, previewStart, previewEnd, previewLimit)
						if previewErr != nil {
							s.logger.Warn("refresh building partition preview failed",
								zap.Error(previewErr),
								zap.String("datasource", datasource.Name),
								zap.String("service", displayServiceName(serviceName)),
								zap.String("day", cacheDay(day).Format("2006-01-02")),
							)
						} else if len(previewRows) > 0 {
							mergedRows, mergePartial := s.mergeSourceRowsLimited(partition.Rows, previewRows, datasource, serviceName, day, "building-preview")
							trimmedRows, trimPartial := s.trimRowsForPartition(mergedRows, datasource, serviceName, day, "building-preview")
							partition.Rows = trimmedRows
							partition.Meta = s.cache.PreparePendingLogPartitionMeta(day, serviceName, datasource, len(trimmedRows), now)
							partition.Meta.Partial = partition.Meta.Partial || mergePartial || trimPartial
							if err := s.cache.StoreLogPartition(partition); err != nil {
								s.logger.Warn("store building partition preview failed",
									zap.Error(err),
									zap.String("datasource", datasource.Name),
									zap.String("service", displayServiceName(serviceName)),
									zap.String("day", cacheDay(day).Format("2006-01-02")),
								)
							} else {
								previewRefreshed = true
								s.logger.Info("refreshed preview rows for building partition",
									zap.String("datasource", datasource.Name),
									zap.String("service", displayServiceName(serviceName)),
									zap.String("day", cacheDay(day).Format("2006-01-02")),
									zap.Int("rows", len(partition.Rows)),
								)
							}
						}
					}
				}
				if s.cache.LogPartitionBuildStale(partition.Meta, now) && !s.isPartitionSyncActive(day, datasource, serviceName) {
					task, created := s.startPartitionSync(day, datasource, serviceName, "query-build-retry", partitionSyncQueueInteractive, func(syncCtx context.Context) error {
						_, _, buildErr := s.buildPartitionFromSource(syncCtx, datasource, snapshot, tagDefinitions, serviceName, day, time.Now().UTC())
						return buildErr
					})
					if created {
						s.logger.Info("requeueing stale building log partition",
							zap.String("datasource", datasource.Name),
							zap.String("service", displayServiceName(serviceName)),
							zap.String("day", cacheDay(day).Format("2006-01-02")),
						)
					} else if task != nil {
						s.logger.Info("building log partition still pending",
							zap.String("datasource", datasource.Name),
							zap.String("service", displayServiceName(serviceName)),
							zap.String("day", cacheDay(day).Format("2006-01-02")),
							zap.String("queue", string(task.queue)),
							zap.Bool("started", task.started),
						)
					}
				}
				if previewRefreshed {
					return partition, false, true, nil
				}
				return partition, false, true, nil
			}
			if s.cache.LogPartitionNeedsRefresh(partition.Meta, day, now) {
				s.logger.Info("queueing hot log partition refresh",
					zap.String("datasource", datasource.Name),
					zap.String("service", displayServiceName(serviceName)),
					zap.String("day", cacheDay(day).Format("2006-01-02")),
					zap.Time("last_sync_at", partition.Meta.LastSyncAt),
				)
				s.startPartitionSync(day, datasource, serviceName, "query-refresh", partitionSyncQueueInteractive, func(syncCtx context.Context) error {
					_, _, refreshErr := s.refreshPartitionIncremental(syncCtx, datasource, snapshot, tagDefinitions, serviceName, day, time.Now().UTC(), partition)
					return refreshErr
				})
				return partition, true, true, nil
			}
			return partition, true, partition.Meta.Partial, nil
		}
	}

	if useCache {
		if !s.cfg.BackgroundSyncEnabled {
			previewPartition, previewPartial, previewErr := s.buildPreviewOnlyPartition(ctx, datasource, snapshot, tagDefinitions, serviceName, day, windowStart, windowEnd, now, previewLimit, true)
			if previewErr != nil {
				return cache.LocalLogPartition{}, false, true, previewErr
			}
			if len(previewPartition.Rows) > 0 {
				s.logger.Info("prepared preview-only log partition",
					zap.String("datasource", datasource.Name),
					zap.String("service", displayServiceName(serviceName)),
					zap.String("day", cacheDay(day).Format("2006-01-02")),
					zap.Int("rows", len(previewPartition.Rows)),
				)
			}
			return previewPartition, false, previewPartial, nil
		}
		previewRows := []model.SearchResult{}
		if previewLimit > 0 {
			previewStart, previewEnd := intersectWindowWithDay(day, windowStart, windowEnd, now)
			if previewEnd.After(previewStart) {
				previewRows, _ = s.fetchPreviewSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, previewStart, previewEnd, previewLimit)
			}
		}
		previewRows, _ = s.normalizeRowsForPartition(previewRows, datasource, serviceName, day, "preview")
		placeholder := cache.LocalLogPartition{
			Meta: s.cache.PreparePendingLogPartitionMeta(day, serviceName, datasource, len(previewRows), now),
			Rows: previewRows,
		}
		if len(previewRows) > 0 {
			s.logger.Info("prepared preview rows while partition is building",
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
				zap.Int("rows", len(previewRows)),
			)
		}
		if err := s.cache.StoreLogPartition(placeholder); err != nil {
			s.logger.Warn("store pending log partition placeholder failed",
				zap.Error(err),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
			)
		}
		task, created := s.startPartitionSync(day, datasource, serviceName, "query-build", partitionSyncQueueInteractive, func(syncCtx context.Context) error {
			_, _, buildErr := s.buildPartitionFromSource(syncCtx, datasource, snapshot, tagDefinitions, serviceName, day, time.Now().UTC())
			return buildErr
		})
		if created {
			s.logger.Info("queueing log partition build from source",
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
			)
		} else if task != nil {
			s.logger.Info("log partition build already pending",
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
				zap.String("queue", string(task.queue)),
				zap.Bool("started", task.started),
			)
		}
		return placeholder, false, true, nil
	}

	s.logger.Info("building log partition from source synchronously",
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

func (s *Service) buildPreviewOnlyPartition(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	day, windowStart, windowEnd, now time.Time,
	previewLimit int,
	persist bool,
) (cache.LocalLogPartition, bool, error) {
	previewRows := []model.SearchResult{}
	if previewLimit > 0 {
		previewStart, previewEnd := intersectWindowWithDay(day, windowStart, windowEnd, now)
		if previewEnd.After(previewStart) {
			var previewErr error
			previewRows, previewErr = s.fetchPreviewSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, previewStart, previewEnd, previewLimit)
			if previewErr != nil {
				return cache.LocalLogPartition{}, true, previewErr
			}
		}
	}
	previewRows, _ = s.normalizeRowsForPartition(previewRows, datasource, serviceName, day, "preview-only")
	partition := cache.LocalLogPartition{
		Meta: s.cache.PrepareLogPartitionMeta(day, serviceName, datasource, len(previewRows), now, true),
		Rows: previewRows,
	}
	if persist {
		if err := s.cache.StoreLogPartition(partition); err != nil {
			s.logger.Warn("store preview-only log partition failed",
				zap.Error(err),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
			)
		}
	}
	return partition, true, nil
}

func (s *Service) buildPartitionFromSource(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	day, now time.Time,
) (cache.LocalLogPartition, bool, error) {
	buildStartedAt := time.Now()
	s.logger.Info("partition build started",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.String("day", cacheDay(day).Format("2006-01-02")),
	)
	rangeStart, rangeEnd := dayBounds(day, now)
	rows, partial, err := s.fetchCompleteSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, rangeStart, rangeEnd)
	if err != nil {
		return cache.LocalLogPartition{}, partial, err
	}
	rows, guardPartial := s.normalizeRowsForPartition(rows, datasource, serviceName, day, "build")
	partial = partial || guardPartial
	partition := cache.LocalLogPartition{
		Meta: s.cache.PrepareLogPartitionMeta(day, serviceName, datasource, len(rows), now, partial),
		Rows: rows,
	}
	persistStartedAt := time.Now()
	if err := s.cache.StoreLogPartition(partition); err != nil {
		s.logger.Warn("store log partition failed",
			zap.Error(err),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
		)
	}
	s.logger.Info("partition build completed",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.String("day", cacheDay(day).Format("2006-01-02")),
		zap.Int("rows", len(partition.Rows)),
		zap.Bool("partial", partial),
		zap.Bool("skip_persist", false),
		zap.Duration("persist_took", time.Since(persistStartedAt)),
		zap.Duration("took", time.Since(buildStartedAt)),
	)
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
	refreshStartedAt := time.Now()
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
	partition.Rows, partial = s.mergePartitionRows(partition.Rows, rows, datasource, serviceName, day, partial)
	partition.Meta = s.cache.PrepareLogPartitionMeta(day, serviceName, datasource, len(partition.Rows), now, partition.Meta.Partial || partial)
	persistStartedAt := time.Now()
	if err := s.cache.StoreLogPartition(partition); err != nil {
		s.logger.Warn("store refreshed hot partition failed",
			zap.Error(err),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
		)
	}
	s.logger.Info("partition refresh completed",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.String("day", cacheDay(day).Format("2006-01-02")),
		zap.Int("rows", len(partition.Rows)),
		zap.Bool("partial", partition.Meta.Partial),
		zap.Duration("persist_took", time.Since(persistStartedAt)),
		zap.Duration("took", time.Since(refreshStartedAt)),
	)
	return partition, partition.Meta.Partial, nil
}

func (s *Service) fetchPreviewSourceRange(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end time.Time,
	limit int,
) ([]model.SearchResult, error) {
	if !end.After(start) || limit <= 0 {
		return []model.SearchResult{}, nil
	}
	logsql := buildSourceLogsQL(datasource, snapshot, serviceName)
	rows, _, err := s.fetchDatasourceWindow(ctx, datasource, snapshot, tagDefinitions, logsql, start, end, limit)
	if err != nil {
		return nil, err
	}
	rows, _ = s.normalizeRowsForPartition(rows, datasource, serviceName, cacheDay(start), "preview")
	return rows, nil
}

func (s *Service) fetchDatasourcePreviewWindow(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	start, end time.Time,
	limit int,
) ([]model.SearchResult, bool, error) {
	if !end.After(start) || limit <= 0 {
		return []model.SearchResult{}, false, nil
	}
	rows, truncated, err := s.fetchDatasourceWindow(
		ctx,
		datasource,
		snapshot,
		tagDefinitions,
		buildSourceLogsQL(datasource, snapshot, ""),
		start,
		end,
		limit,
	)
	if err != nil {
		return nil, true, err
	}
	rows, guardPartial := s.normalizeRowsForPartition(rows, datasource, "", cacheDay(start), "datasource-preview")
	return rows, truncated || guardPartial, nil
}

func (s *Service) fetchAllServicePreviewWindow(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	start, end time.Time,
	perServiceLimit int,
) ([]model.SearchResult, bool, error) {
	entries, err := s.store.ListServiceCatalog(ctx, datasource.ID)
	if err != nil {
		return s.fetchDatasourcePreviewWindow(ctx, datasource, snapshot, tagDefinitions, start, end, perServiceLimit)
	}
	serviceNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.ServiceName) == "" {
			continue
		}
		serviceNames = append(serviceNames, entry.ServiceName)
	}
	serviceNames = uniqueStrings(serviceNames)
	if len(serviceNames) == 0 {
		return s.fetchDatasourcePreviewWindow(ctx, datasource, snapshot, tagDefinitions, start, end, perServiceLimit)
	}

	type previewResult struct {
		rows []model.SearchResult
		err  error
	}

	workerLimit := s.serviceChunkConcurrency()
	if workerLimit <= 0 {
		workerLimit = 1
	}
	if workerLimit > 6 {
		workerLimit = 6
	}
	sem := make(chan struct{}, workerLimit)
	resultsCh := make(chan previewResult, len(serviceNames))
	var wg sync.WaitGroup

	for _, serviceName := range serviceNames {
		name := serviceName
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rows, fetchErr := s.fetchPreviewSourceRange(ctx, datasource, snapshot, tagDefinitions, name, start, end, perServiceLimit)
			resultsCh <- previewResult{rows: rows, err: fetchErr}
		}()
	}

	wg.Wait()
	close(resultsCh)

	merged := make([]model.SearchResult, 0, len(serviceNames)*maxSearchInt(1, perServiceLimit))
	partial := false
	for item := range resultsCh {
		if item.err != nil {
			partial = true
			continue
		}
		merged = append(merged, item.rows...)
	}
	sortSearchResults(merged)
	merged = limitResultsPerService(merged, perServiceLimit)
	maxVisible := responseVisibleLimit(perServiceLimit, merged)
	if len(merged) > maxVisible {
		merged = merged[:maxVisible]
		partial = true
	}
	return merged, partial, nil
}

func (s *Service) fetchCompleteSourceRange(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end time.Time,
) ([]model.SearchResult, bool, error) {
	concurrency := s.serviceChunkConcurrency()
	var chunkSem chan struct{}
	if concurrency > 1 {
		chunkSem = make(chan struct{}, concurrency-1)
	}
	rows, partial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, 0, chunkSem)
	if err != nil {
		return nil, partial, err
	}
	rows, guardPartial := s.normalizeRowsForPartition(rows, datasource, serviceName, cacheDay(start), "source-range")
	return rows, partial || guardPartial, nil
}

func (s *Service) fetchSourceRangeRecursive(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end time.Time,
	depth int,
	chunkSem chan struct{},
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
		rows, guardPartial := s.dedupeRowsLimited(rows, datasource, serviceName, cacheDay(start), "recursive-base")
		return rows, guardPartial, nil
	}
	if depth >= s.maxSourceSplitDepth() {
		return s.fetchDenseSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, depth, "maximum split depth")
	}
	if end.Sub(start) <= s.sourceMinSplitWindow() {
		return s.fetchDenseSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, depth, "minimum split window")
	}

	mid := start.Add(end.Sub(start) / 2)
	if !mid.After(start) || !end.After(mid) {
		return s.fetchDenseSourceRange(ctx, datasource, snapshot, tagDefinitions, serviceName, start, end, depth, "invalid split midpoint")
	}
	s.logger.Debug("splitting truncated source window",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.Time("start", start),
		zap.Time("mid", mid),
		zap.Time("end", end),
		zap.Int("depth", depth),
	)
	if !s.tryFetchSplitConcurrently(chunkSem) {
		leftRows, leftPartial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, start, mid, depth+1, chunkSem)
		if err != nil {
			return nil, false, err
		}
		rightRows, rightPartial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, mid, end, depth+1, chunkSem)
		if err != nil {
			return nil, false, err
		}
		mergedRows, guardPartial := s.mergeSourceRowsLimited(leftRows, rightRows, datasource, serviceName, cacheDay(start), "recursive-merge")
		return mergedRows, leftPartial || rightPartial || guardPartial, nil
	}

	type splitResult struct {
		rows    []model.SearchResult
		partial bool
		err     error
	}
	leftCh := make(chan splitResult, 1)
	go func() {
		defer s.releaseFetchSplit(chunkSem)
		rows, partial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, start, mid, depth+1, chunkSem)
		leftCh <- splitResult{rows: rows, partial: partial, err: err}
	}()
	rightRows, rightPartial, err := s.fetchSourceRangeRecursive(ctx, datasource, snapshot, tagDefinitions, serviceName, mid, end, depth+1, chunkSem)
	leftResult := <-leftCh
	if leftResult.err != nil {
		return nil, false, leftResult.err
	}
	if err != nil {
		return nil, false, err
	}
	leftRows, leftPartial := leftResult.rows, leftResult.partial
	mergedRows, guardPartial := s.mergeSourceRowsLimited(leftRows, rightRows, datasource, serviceName, cacheDay(start), "recursive-merge")
	return mergedRows, leftPartial || rightPartial || guardPartial, nil
}

func (s *Service) resolveServiceTargets(ctx context.Context, datasourceID string, requested []string) ([]string, error) {
	if items := uniqueStrings(requested); len(items) > 0 {
		return items, nil
	}
	return []string{""}, nil
}

func (s *Service) serviceChunkConcurrency() int {
	if s.cfg.ServiceChunkConcurrency > 0 {
		return s.cfg.ServiceChunkConcurrency
	}
	return 1
}

func (s *Service) maxSourceSplitDepth() int {
	return 10
}

func (s *Service) sourceMinSplitWindow() time.Duration {
	return 250 * time.Millisecond
}

func (s *Service) buildingPreviewRefreshInterval() time.Duration {
	return 5 * time.Second
}

func (s *Service) shouldRefreshBuildingPreview(meta cache.LocalLogPartitionMeta, now time.Time) bool {
	if !meta.Building {
		return false
	}
	if meta.LastSyncAt.IsZero() {
		return true
	}
	return now.UTC().Sub(meta.LastSyncAt.UTC()) >= s.buildingPreviewRefreshInterval()
}

func (s *Service) denseSourceWindowLimit() int {
	limit := s.cfg.DenseWindowLimit
	if limit <= 0 {
		limit = 5000
	}
	maxWindow := s.sourceWindowLimit()
	if maxWindow > 0 && limit > maxWindow {
		limit = maxWindow
	}
	if maxPartitionRows := s.partitionRowLimit(); maxPartitionRows > 0 && limit > maxPartitionRows {
		limit = maxPartitionRows
	}
	if limit <= 0 {
		return 5000
	}
	return limit
}

func (s *Service) fetchDenseSourceRange(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	start, end time.Time,
	depth int,
	reason string,
) ([]model.SearchResult, bool, error) {
	logsql := buildSourceLogsQL(datasource, snapshot, serviceName)
	rows, truncated, err := s.fetchDatasourceWindow(
		ctx,
		datasource,
		snapshot,
		tagDefinitions,
		logsql,
		start,
		end,
		s.denseSourceWindowLimit(),
	)
	if err != nil {
		return nil, true, err
	}
	rows, guardPartial := s.dedupeRowsLimited(rows, datasource, serviceName, cacheDay(start), "dense-drain")
	if truncated || guardPartial {
		s.logger.Warn("source range still truncated after dense window drain",
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.Time("start", start),
			zap.Time("end", end),
			zap.Int("depth", depth),
			zap.String("reason", reason),
			zap.Int("rows", len(rows)),
			zap.Int("limit", s.denseSourceWindowLimit()),
		)
		return rows, true, nil
	}
	s.logger.Info("dense source window drain completed",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.Time("start", start),
		zap.Time("end", end),
		zap.Int("depth", depth),
		zap.String("reason", reason),
		zap.Int("rows", len(rows)),
	)
	return rows, false, nil
}

func (s *Service) tryFetchSplitConcurrently(chunkSem chan struct{}) bool {
	if chunkSem == nil {
		return false
	}
	select {
	case chunkSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Service) releaseFetchSplit(chunkSem chan struct{}) {
	if chunkSem == nil {
		return
	}
	select {
	case <-chunkSem:
	default:
	}
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
	maxRows := requestedWindow
	if guardLimit := s.rowsBeforePartialLimit(); guardLimit > 0 && maxRows > guardLimit {
		maxRows = guardLimit
	}

	normalizedRows := make([]model.SearchResult, 0, maxSearchInt(1, maxRows))
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
		s.logger.Debug("source query chunk fetched",
			zap.String("datasource", datasource.Name),
			zap.Int("offset", offset),
			zap.Int("limit", limit),
			zap.Int("rows", len(rows)),
			zap.Time("start", start),
			zap.Time("end", end),
		)
		if len(rows) == 0 {
			break
		}
		lastBatchSize = len(rows)

		for _, row := range rows {
			normalizedRows = append(normalizedRows, normalizeRow(datasource, snapshot, tagDefinitions, row))
			if len(normalizedRows) >= maxRows {
				truncated = true
				s.logger.Debug("source window limited by safety guard",
					zap.String("datasource", datasource.Name),
					zap.Time("start", start),
					zap.Time("end", end),
					zap.Int("rows", len(normalizedRows)),
					zap.Int("guard_limit", maxRows),
				)
				break
			}
		}
		offset += len(rows)

		if truncated || len(rows) < limit {
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
	if strings.ToLower(strings.TrimSpace(normalized.KeywordMode)) == "or" {
		normalized.KeywordMode = "or"
	} else {
		normalized.KeywordMode = "and"
	}
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
		Service:    stringify(extractValue(row, serviceField, "app", "service", "service_name", "job")),
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

func (s *Service) normalizeRowsForPartition(rows []model.SearchResult, datasource model.Datasource, serviceName string, day time.Time, stage string) ([]model.SearchResult, bool) {
	filtered, partial := s.dedupeRowsLimited(rows, datasource, serviceName, day, stage)
	filtered, sortPartial := s.sortSearchResultsLimited(filtered, datasource, serviceName, day, stage)
	return filtered, partial || sortPartial
}

func (s *Service) mergePartitionRows(existing []model.SearchResult, incoming []model.SearchResult, datasource model.Datasource, serviceName string, day time.Time, partial bool) ([]model.SearchResult, bool) {
	merged, mergePartial := s.mergeSourceRowsLimited(existing, incoming, datasource, serviceName, day, "partition-refresh")
	merged, normalizePartial := s.normalizeRowsForPartition(merged, datasource, serviceName, day, "partition-refresh")
	trimmed, trimPartial := s.trimRowsForPartition(merged, datasource, serviceName, day, "partition-refresh")
	return trimmed, partial || mergePartial || normalizePartial || trimPartial
}

func (s *Service) dedupeRowsLimited(rows []model.SearchResult, datasource model.Datasource, serviceName string, day time.Time, stage string) ([]model.SearchResult, bool) {
	limit := s.maxDedupeRowsLimit()
	partial := false
	if limit > 0 && len(rows) > limit {
		partial = true
		s.logger.Warn("partition rows limited before dedupe",
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
			zap.String("stage", stage),
			zap.Int("rows", len(rows)),
			zap.Int("guard_limit", limit),
		)
		rows = rows[:limit]
	}
	filtered := dedupeRows(rows)
	trimmed, trimPartial := s.trimRowsForPartition(filtered, datasource, serviceName, day, stage)
	return trimmed, partial || trimPartial
}

func (s *Service) sortSearchResultsLimited(rows []model.SearchResult, datasource model.Datasource, serviceName string, day time.Time, stage string) ([]model.SearchResult, bool) {
	limit := s.maxSortRowsLimit()
	partial := false
	if limit > 0 && len(rows) > limit {
		partial = true
		s.logger.Warn("partition rows limited before sort",
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
			zap.String("stage", stage),
			zap.Int("rows", len(rows)),
			zap.Int("guard_limit", limit),
		)
		rows = rows[:limit]
	}
	sortSearchResults(rows)
	return rows, partial
}

func (s *Service) mergeSourceRowsLimited(leftRows []model.SearchResult, rightRows []model.SearchResult, datasource model.Datasource, serviceName string, day time.Time, stage string) ([]model.SearchResult, bool) {
	limit := s.maxDedupeRowsLimit()
	if limit <= 0 {
		limit = len(leftRows) + len(rightRows)
	}
	filtered := make([]model.SearchResult, 0, minSearchInt(limit, len(leftRows)+len(rightRows)))
	seen := make(map[string]struct{}, minSearchInt(limit, len(leftRows)+len(rightRows)))
	partial := false

	appendRows := func(items []model.SearchResult) {
		for _, row := range items {
			key := searchResultKey(row)
			if _, ok := seen[key]; ok {
				continue
			}
			if len(filtered) >= limit {
				partial = true
				return
			}
			seen[key] = struct{}{}
			filtered = append(filtered, row)
		}
	}

	appendRows(rightRows)
	appendRows(leftRows)
	if partial {
		s.logger.Warn("partition rows limited during merge",
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
			zap.String("stage", stage),
			zap.Int("left_rows", len(leftRows)),
			zap.Int("right_rows", len(rightRows)),
			zap.Int("guard_limit", limit),
		)
	}
	return filtered, partial
}

func (s *Service) trimRowsForPartition(rows []model.SearchResult, datasource model.Datasource, serviceName string, day time.Time, stage string) ([]model.SearchResult, bool) {
	limit := s.partitionRowLimit()
	if limit <= 0 || len(rows) <= limit {
		return rows, false
	}
	s.logger.Warn("partition build downgraded due to row threshold",
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.String("day", cacheDay(day).Format("2006-01-02")),
		zap.String("stage", stage),
		zap.Int("rows", len(rows)),
		zap.Int("guard_limit", limit),
	)
	return rows[:limit], true
}

func (s *Service) rowsBeforePartialLimit() int {
	if s.cfg.MaxRowsBeforePartial > 0 {
		return s.cfg.MaxRowsBeforePartial
	}
	return 8000
}

func (s *Service) partitionRowLimit() int {
	if s.cfg.MaxPartitionRows > 0 {
		return s.cfg.MaxPartitionRows
	}
	return 10000
}

func (s *Service) maxDedupeRowsLimit() int {
	if s.cfg.MaxDedupeRows > 0 {
		return s.cfg.MaxDedupeRows
	}
	return maxSearchInt(12000, s.partitionRowLimit())
}

func (s *Service) maxSortRowsLimit() int {
	if s.cfg.MaxSortRows > 0 {
		return s.cfg.MaxSortRows
	}
	return maxSearchInt(12000, s.rowsBeforePartialLimit())
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

func sortSearchResults(rows []model.SearchResult) {
	sort.Slice(rows, func(i, j int) bool {
		left := parseTimestamp(rows[i].Timestamp)
		right := parseTimestamp(rows[j].Timestamp)
		if left.Equal(right) {
			leftMessage := strings.ToLower(rows[i].Message)
			rightMessage := strings.ToLower(rows[j].Message)
			if leftMessage == rightMessage {
				if rows[i].Datasource == rows[j].Datasource {
					return rows[i].Service < rows[j].Service
				}
				return rows[i].Datasource < rows[j].Datasource
			}
			return leftMessage < rightMessage
		}
		return left.After(right)
	})
}

func limitResultsPerService(rows []model.SearchResult, perServiceLimit int) []model.SearchResult {
	if perServiceLimit <= 0 || len(rows) <= perServiceLimit {
		return rows
	}
	grouped := make([]model.SearchResult, 0, len(rows))
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		key := row.Datasource + "\x00" + firstNonEmpty(row.Service, "__all__")
		if counts[key] >= perServiceLimit {
			continue
		}
		counts[key]++
		grouped = append(grouped, row)
	}
	return grouped
}

func responseVisibleLimit(pageSize int, rows []model.SearchResult) int {
	if pageSize <= 0 {
		pageSize = 500
	}
	groups := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := row.Datasource + "\x00" + firstNonEmpty(row.Service, "__all__")
		groups[key] = struct{}{}
	}
	limit := pageSize * maxSearchInt(1, len(groups))
	if limit > 10000 {
		limit = 10000
	}
	if limit <= 0 {
		return pageSize
	}
	return limit
}

func minSearchInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxSearchInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func applySearchFilters(rows []model.SearchResult, req model.SearchRequest) []model.SearchResult {
	if len(rows) == 0 {
		return rows
	}
	keywords := splitKeywords(req.Keyword)
	filtered := make([]model.SearchResult, 0, len(rows))
	for _, row := range rows {
		if !matchesKeywordFilter(row, keywords, req.KeywordMode) {
			continue
		}
		if !matchesTagFilters(row, req.Tags) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func splitKeywords(raw string) []string {
	return uniqueStrings(strings.FieldsFunc(raw, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ',' || r == ';'
	}))
}

func matchesKeywordFilter(row model.SearchResult, keywords []string, mode string) bool {
	if len(keywords) == 0 {
		return true
	}
	haystack := searchableRowText(row)
	if mode == "or" {
		for _, token := range keywords {
			if strings.Contains(haystack, strings.ToLower(strings.Trim(strings.TrimSpace(token), `"'`))) {
				return true
			}
		}
		return false
	}
	for _, token := range keywords {
		if !strings.Contains(haystack, strings.ToLower(strings.Trim(strings.TrimSpace(token), `"'`))) {
			return false
		}
	}
	return true
}

func matchesTagFilters(row model.SearchResult, filters map[string][]string) bool {
	if len(filters) == 0 {
		return true
	}
	for field, values := range filters {
		normalizedValues := uniqueStrings(values)
		if len(normalizedValues) == 0 {
			continue
		}
		candidates := make([]string, 0, 2)
		if row.Labels != nil && row.Labels[field] != "" {
			candidates = append(candidates, strings.ToLower(strings.TrimSpace(row.Labels[field])))
		}
		if row.Raw != nil {
			if rawValue, ok := row.Raw[field]; ok {
				candidates = append(candidates, strings.ToLower(strings.TrimSpace(stringify(rawValue))))
			}
		}
		if len(candidates) == 0 {
			return false
		}
		matched := false
		for _, value := range normalizedValues {
			needle := strings.ToLower(strings.TrimSpace(value))
			for _, candidate := range candidates {
				if candidate == needle || strings.Contains(candidate, needle) {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func searchableRowText(row model.SearchResult) string {
	parts := []string{
		row.Message,
		row.Service,
		row.Pod,
		row.Datasource,
		row.Timestamp,
	}
	for key, value := range row.Labels {
		parts = append(parts, key, value)
	}
	if len(row.Raw) > 0 {
		if raw, err := json.Marshal(row.Raw); err == nil {
			parts = append(parts, string(raw))
		}
	}
	return strings.ToLower(strings.Join(parts, "\n"))
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

func intersectWindowWithDay(day, windowStart, windowEnd, now time.Time) (time.Time, time.Time) {
	dayStart, dayEnd := dayBounds(day, now)
	if windowStart.After(dayStart) {
		dayStart = windowStart
	}
	if windowEnd.Before(dayEnd) {
		dayEnd = windowEnd
	}
	if dayEnd.Before(dayStart) {
		return dayStart, dayStart
	}
	return dayStart, dayEnd
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
