package query

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/model"
)

func (s *Service) StartMaintenance(ctx context.Context) {
	if s.cache == nil || strings.TrimSpace(s.cfg.LocalLogDir) == "" {
		return
	}

	s.logger.Info("starting query maintenance workers",
		zap.String("log_dir", s.cfg.LocalLogDir),
		zap.Int("hot_days", s.cfg.LocalLogHotDays),
		zap.Duration("refresh_interval", s.cfg.LocalLogRefreshInterval),
		zap.String("daily_check_at", strings.TrimSpace(s.cfg.LocalLogDailyCheckAt)),
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
	concurrency := s.cfg.LocalLogCheckConcurrency
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

		serviceNames := uniqueServiceNames(services)
		if len(serviceNames) == 0 {
			s.logger.Debug("hot cache check skipped datasource with empty service catalog",
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
			_, _, err = s.refreshPartitionIncremental(ctx, datasource, snapshot, tagDefinitions, serviceName, day, now, partition)
			return err
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
	_, _, err = s.buildPartitionFromSource(ctx, datasource, snapshot, tagDefinitions, serviceName, day, now)
	return err
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
