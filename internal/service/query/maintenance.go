package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/model"
)

func (s *Service) StartMaintenance(ctx context.Context) {
	if s.cache == nil {
		s.logger.Info("query maintenance disabled: cache service unavailable")
		return
	}
	if strings.TrimSpace(s.cfg.LocalLogDir) == "" {
		s.logger.Info("query maintenance disabled: local_log_dir is empty")
		return
	}
	if s.cfg.LocalLogRefreshInterval <= 0 {
		s.logger.Info("query maintenance disabled: local_log_refresh_interval is not positive",
			zap.Duration("refresh_interval", s.cfg.LocalLogRefreshInterval),
		)
		return
	}

	s.logger.Info("starting query maintenance workers",
		zap.String("log_dir", s.cfg.LocalLogDir),
		zap.Int("hot_days", s.cfg.LocalLogHotDays),
		zap.Duration("refresh_interval", s.cfg.LocalLogRefreshInterval),
		zap.String("daily_check_at", strings.TrimSpace(s.cfg.LocalLogDailyCheckAt)),
		zap.Int("interactive_sync_concurrency", s.cfg.InteractiveSyncConcurrency),
		zap.Int("maintenance_sync_concurrency", s.cfg.MaintenanceSyncConcurrency),
		zap.Int("service_chunk_concurrency", s.cfg.ServiceChunkConcurrency),
		zap.Duration("interactive_service_ttl", s.cfg.InteractiveServiceTTL),
	)

	go s.runStartupCacheCheck(ctx)
	go s.StartHotSync(ctx)
	if strings.TrimSpace(s.cfg.LocalLogDailyCheckAt) != "" {
		go s.runDailyCacheCheckLoop(ctx)
	}
}

func (s *Service) runStartupCacheCheck(ctx context.Context) {
	if err := s.ensureHotCache(ctx, "startup"); err != nil {
		s.logger.Warn("startup cache check failed", zap.Error(err))
	}
}

func (s *Service) runDailyCacheCheckLoop(ctx context.Context) {
	for {
		nextRun, err := s.nextDailyCacheCheck(time.Now())
		if err != nil {
			s.logger.Warn("daily cache check disabled", zap.Error(err))
			return
		}
		wait := time.Until(nextRun)
		s.logger.Info("scheduled next daily cache check",
			zap.Time("next_run", nextRun),
			zap.Duration("wait", wait),
		)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		if err := s.ensureHotCache(ctx, "daily"); err != nil {
			s.logger.Warn("daily cache check failed", zap.Error(err))
		}
	}
}

func (s *Service) nextDailyCacheCheck(now time.Time) (time.Time, error) {
	clock := strings.TrimSpace(s.cfg.LocalLogDailyCheckAt)
	if clock == "" {
		return time.Time{}, fmt.Errorf("daily check time not configured")
	}
	parsed, err := time.Parse("15:04", clock)
	if err != nil {
		return time.Time{}, err
	}
	localNow := now.In(time.Local)
	next := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), parsed.Hour(), parsed.Minute(), 0, 0, time.Local)
	if !next.After(localNow) {
		next = next.Add(24 * time.Hour)
	}
	return next, nil
}

func (s *Service) ensureHotCache(ctx context.Context, reason string) error {
	datasources, err := s.store.ListDatasources(ctx, true)
	if err != nil {
		return err
	}
	if len(datasources) == 0 {
		s.logger.Debug("hot cache check skipped: no enabled datasource", zap.String("reason", reason))
		return nil
	}

	tagDefinitions, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	days := s.hotCacheDays(now)
	trackedServices, trackedErr := s.trackedHotServicesByDatasource()
	if trackedErr != nil {
		s.logger.Warn("list tracked log partitions for hot cache check failed",
			zap.Error(trackedErr),
			zap.String("reason", reason),
		)
	}
	concurrency := s.cfg.MaintenanceSyncConcurrency
	if concurrency <= 0 {
		concurrency = s.cfg.LocalLogCheckConcurrency
	}
	if concurrency <= 0 {
		concurrency = 4
	}

	s.logger.Info("running hot cache check",
		zap.String("reason", reason),
		zap.Int("datasource_count", len(datasources)),
		zap.Int("day_count", len(days)),
		zap.Int("concurrency", concurrency),
	)

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var firstErrMu sync.Mutex

	for _, datasource := range datasources {
		ds := datasource
		services, listErr := s.store.ListServiceCatalog(ctx, ds.ID)
		if listErr != nil {
			s.logger.Warn("list service catalog for hot cache check failed",
				zap.Error(listErr),
				zap.String("datasource", ds.Name),
				zap.String("reason", reason),
			)
			if firstErr == nil {
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = listErr
				}
				firstErrMu.Unlock()
			}
			continue
		}

		serviceNames := uniqueStrings(append(uniqueServiceNames(services), trackedServices[ds.ID]...))
		if len(serviceNames) == 0 {
			discovered, discoverErr := s.discoverServiceNamesForHotCache(ctx, ds, days, now)
			if discoverErr != nil {
				s.logger.Warn("discover service catalog for hot cache check failed",
					zap.Error(discoverErr),
					zap.String("datasource", ds.Name),
					zap.String("reason", reason),
				)
			}
			serviceNames = uniqueStrings(append(serviceNames, discovered...))
		}
		if len(serviceNames) == 0 {
			s.logger.Info("hot cache check skipped datasource with no known services",
				zap.String("datasource", ds.Name),
				zap.String("reason", reason),
			)
			continue
		}

		snapshot, _ := s.store.GetSnapshot(ctx, ds.ID)
		for _, serviceName := range serviceNames {
			for _, day := range days {
				sem <- struct{}{}
				wg.Add(1)
				go func(ds model.Datasource, snapshot model.DatasourceTagSnapshot, serviceName string, day time.Time) {
					defer func() {
						<-sem
						wg.Done()
					}()
					if partErr := s.ensureHotPartition(ctx, ds, snapshot, tagDefinitions, serviceName, day, now, reason); partErr != nil {
						s.logger.Warn("hot cache partition sync failed",
							zap.Error(partErr),
							zap.String("reason", reason),
							zap.String("datasource", ds.Name),
							zap.String("service", displayServiceName(serviceName)),
							zap.String("day", cacheDay(day).Format("2006-01-02")),
						)
						firstErrMu.Lock()
						if firstErr == nil {
							firstErr = partErr
						}
						firstErrMu.Unlock()
					}
				}(ds, snapshot, serviceName, day)
			}
		}
	}

	wg.Wait()
	if firstErr == nil {
		s.logger.Info("hot cache check completed",
			zap.String("reason", reason),
		)
	}
	return firstErr
}

func (s *Service) ensureHotPartition(
	ctx context.Context,
	datasource model.Datasource,
	snapshot model.DatasourceTagSnapshot,
	tagDefinitions []model.TagDefinition,
	serviceName string,
	day, now time.Time,
	reason string,
) error {
	partition, hit, err := s.cache.LoadLogPartition(day, serviceName, datasource)
	if err != nil {
		return err
	}
	if hit {
		if s.cache.LogPartitionNeedsRefresh(partition.Meta, day, now) {
			s.logger.Info("refreshing current-day hot partition",
				zap.String("reason", reason),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
			)
			queue := s.preferredSyncQueue(datasource, serviceName, now, partitionSyncQueueMaintenance)
			task, _ := s.startPartitionSync(day, datasource, serviceName, "hot-refresh-"+reason, queue, func(syncCtx context.Context) error {
				_, _, refreshErr := s.refreshPartitionIncremental(syncCtx, datasource, snapshot, tagDefinitions, serviceName, day, time.Now().UTC(), partition)
				return refreshErr
			})
			return s.waitPartitionSync(ctx, task)
		}
		s.logger.Debug("hot partition already present",
			zap.String("reason", reason),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
			zap.Int("rows", len(partition.Rows)),
		)
		return nil
	}

	s.logger.Info("building missing hot partition",
		zap.String("reason", reason),
		zap.String("datasource", datasource.Name),
		zap.String("service", displayServiceName(serviceName)),
		zap.String("day", cacheDay(day).Format("2006-01-02")),
	)
	queue := s.preferredSyncQueue(datasource, serviceName, now, partitionSyncQueueMaintenance)
	task, _ := s.startPartitionSync(day, datasource, serviceName, "hot-build-"+reason, queue, func(syncCtx context.Context) error {
		_, _, buildErr := s.buildPartitionFromSource(syncCtx, datasource, snapshot, tagDefinitions, serviceName, day, time.Now().UTC())
		return buildErr
	})
	return s.waitPartitionSync(ctx, task)
}

func (s *Service) hotCacheDays(now time.Time) []time.Time {
	count := s.cfg.LocalLogHotDays
	if count <= 0 {
		count = 2
	}
	days := make([]time.Time, 0, count)
	base := cacheDay(now)
	for index := 0; index < count; index++ {
		days = append(days, base.Add(time.Duration(-index)*24*time.Hour))
	}
	return days
}

func uniqueServiceNames(entries []model.ServiceCatalogEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if trimmed := strings.TrimSpace(entry.ServiceName); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	return uniqueStrings(names)
}

func (s *Service) trackedHotServicesByDatasource() (map[string][]string, error) {
	if s.cache == nil {
		return map[string][]string{}, nil
	}
	metas, err := s.cache.ListTrackedLogPartitions()
	if err != nil {
		return nil, err
	}
	result := make(map[string][]string)
	for _, meta := range metas {
		if strings.TrimSpace(meta.DatasourceID) == "" || strings.TrimSpace(meta.Service) == "" {
			continue
		}
		result[meta.DatasourceID] = append(result[meta.DatasourceID], meta.Service)
	}
	for key, items := range result {
		result[key] = uniqueStrings(items)
	}
	return result, nil
}

func (s *Service) discoverServiceNamesForHotCache(
	ctx context.Context,
	datasource model.Datasource,
	days []time.Time,
	now time.Time,
) ([]string, error) {
	if len(days) == 0 {
		return nil, nil
	}
	serviceField := strings.TrimSpace(datasource.FieldMapping.ServiceField)
	if serviceField == "" {
		serviceField = model.DefaultDatasourceFieldMapping().ServiceField
	}

	rangeStart := cacheDay(days[len(days)-1])
	rangeEnd := now
	limit := s.cfg.SourceRequestLimit
	switch {
	case limit <= 0:
		limit = 1000
	case limit > 2000:
		limit = 2000
	}

	req := victorialogs.FieldValuesRequest{
		Query:       "*",
		Field:       serviceField,
		Start:       rangeStart,
		End:         rangeEnd,
		Limit:       limit,
		IgnorePipes: true,
	}

	values, err := s.client.StreamFieldValues(ctx, datasource, req)
	if err != nil {
		values, err = s.client.FieldValues(ctx, datasource, req)
		if err != nil {
			return nil, err
		}
	}

	services := make([]string, 0, len(values))
	for _, item := range values {
		if trimmed := strings.TrimSpace(item.Value); trimmed != "" {
			services = append(services, trimmed)
		}
	}
	services = uniqueStrings(services)
	sort.Strings(services)
	if len(services) == 0 {
		return nil, nil
	}

	if err := s.store.ReplaceServiceCatalog(ctx, datasource.ID, serviceField, services, s.cache.ServiceListTTL()); err != nil {
		s.logger.Warn("persist discovered hot cache service catalog failed",
			zap.Error(err),
			zap.String("datasource", datasource.Name),
			zap.String("service_field", serviceField),
		)
	}
	s.logger.Info("discovered services for hot cache check",
		zap.String("datasource", datasource.Name),
		zap.String("service_field", serviceField),
		zap.Int("service_count", len(services)),
	)
	return services, nil
}
