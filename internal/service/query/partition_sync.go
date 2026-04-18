package query

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/model"
)

type partitionSyncTask struct {
	done      chan struct{}
	err       error
	startedAt time.Time
	reason    string
}

func (s *Service) partitionSyncKey(day time.Time, service string, datasource model.Datasource) string {
	return fmt.Sprintf("%s:%s:%s", datasource.ID, cacheDay(day).Format("2006-01-02"), service)
}

func (s *Service) startPartitionSync(
	day time.Time,
	datasource model.Datasource,
	serviceName string,
	reason string,
	runner func(context.Context) error,
) (*partitionSyncTask, bool) {
	key := s.partitionSyncKey(day, serviceName, datasource)

	s.partitionSyncMu.Lock()
	if existing, ok := s.partitionSyncs[key]; ok {
		s.partitionSyncMu.Unlock()
		s.logger.Debug("partition sync already in progress",
			zap.String("reason", reason),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
		)
		return existing, false
	}

	task := &partitionSyncTask{
		done:      make(chan struct{}),
		startedAt: time.Now().UTC(),
		reason:    reason,
	}
	s.partitionSyncs[key] = task
	s.partitionSyncMu.Unlock()

	go func() {
		if s.partitionSyncSem != nil {
			s.partitionSyncSem <- struct{}{}
			defer func() { <-s.partitionSyncSem }()
		}

		s.logger.Info("partition sync started",
			zap.String("reason", reason),
			zap.String("datasource", datasource.Name),
			zap.String("service", displayServiceName(serviceName)),
			zap.String("day", cacheDay(day).Format("2006-01-02")),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		task.err = runner(ctx)
		if task.err != nil {
			s.logger.Warn("partition sync failed",
				zap.Error(task.err),
				zap.String("reason", reason),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
				zap.Duration("took", time.Since(task.startedAt)),
			)
		} else {
			s.logger.Info("partition sync completed",
				zap.String("reason", reason),
				zap.String("datasource", datasource.Name),
				zap.String("service", displayServiceName(serviceName)),
				zap.String("day", cacheDay(day).Format("2006-01-02")),
				zap.Duration("took", time.Since(task.startedAt)),
			)
		}

		s.partitionSyncMu.Lock()
		delete(s.partitionSyncs, key)
		close(task.done)
		s.partitionSyncMu.Unlock()
	}()

	return task, true
}

func (s *Service) waitPartitionSync(ctx context.Context, task *partitionSyncTask) error {
	if task == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-task.done:
		return task.err
	}
}
