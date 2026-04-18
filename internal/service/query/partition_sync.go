package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/model"
)

type partitionSyncQueue string

const (
	partitionSyncQueueInteractive partitionSyncQueue = "interactive"
	partitionSyncQueueMaintenance partitionSyncQueue = "maintenance"
)

type partitionSyncTask struct {
	done      chan struct{}
	err       error
	startedAt time.Time
	reason    string
	queue     partitionSyncQueue
	key       string
	day       time.Time
	datasource model.Datasource
	serviceName string
	runner    func(context.Context) error
	started   bool
}

func (s *Service) partitionSyncKey(day time.Time, service string, datasource model.Datasource) string {
	return fmt.Sprintf("%s:%s:%s", datasource.ID, cacheDay(day).Format("2006-01-02"), service)
}

func (s *Service) startPartitionSync(
	day time.Time,
	datasource model.Datasource,
	serviceName string,
	reason string,
	queue partitionSyncQueue,
	runner func(context.Context) error,
) (*partitionSyncTask, bool) {
	key := s.partitionSyncKey(day, serviceName, datasource)

	s.partitionSyncMu.Lock()
	if existing, ok := s.partitionSyncs[key]; ok {
		upgraded := false
		if queue == partitionSyncQueueInteractive && !existing.started {
			if existing.queue != partitionSyncQueueInteractive {
				existing.queue = partitionSyncQueueInteractive
				upgraded = true
			}
			s.prependInteractivePendingLocked(key)
			s.tryStartInteractiveUrgentLocked(existing)
			s.partitionSyncCond.Broadcast()
		}
		s.partitionSyncMu.Unlock()
		s.logger.Debug("partition sync already in progress",
			zap.String("reason", reason),
			zap.String("queue", string(existing.queue)),
			zap.Bool("upgraded", upgraded),
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
		queue:     queue,
		key:       key,
		day:       cacheDay(day),
		datasource: datasource,
		serviceName: serviceName,
		runner:    runner,
	}
	s.partitionSyncs[key] = task
	s.enqueuePartitionSyncLocked(task)
	if queue == partitionSyncQueueInteractive && strings.HasPrefix(reason, "query-") {
		s.tryStartInteractiveUrgentLocked(task)
	}
	s.partitionSyncCond.Broadcast()
	s.partitionSyncMu.Unlock()

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

func (s *Service) isPartitionSyncActive(day time.Time, datasource model.Datasource, serviceName string) bool {
	key := s.partitionSyncKey(day, serviceName, datasource)
	s.partitionSyncMu.Lock()
	defer s.partitionSyncMu.Unlock()
	_, ok := s.partitionSyncs[key]
	return ok
}

func (s *Service) startPartitionWorkers(queue partitionSyncQueue, concurrency int) {
	if concurrency <= 0 {
		return
	}
	for workerID := 0; workerID < concurrency; workerID++ {
		go s.runPartitionWorker(queue, workerID)
	}
}

func (s *Service) runPartitionWorker(queue partitionSyncQueue, workerID int) {
	for {
		task := s.nextPartitionSyncTask(queue)
		if task == nil {
			continue
		}
		s.executePartitionSyncTask(task, workerID, false, nil)
	}
}

func (s *Service) nextPartitionSyncTask(queue partitionSyncQueue) *partitionSyncTask {
	s.partitionSyncMu.Lock()
	defer s.partitionSyncMu.Unlock()

	for {
		task := s.dequeuePartitionSyncLocked(queue)
		if task != nil {
			task.started = true
			task.startedAt = time.Now().UTC()
			return task
		}
		s.partitionSyncCond.Wait()
	}
}

func (s *Service) dequeuePartitionSyncLocked(queue partitionSyncQueue) *partitionSyncTask {
	pending := &s.maintenancePendingSyncs
	if queue == partitionSyncQueueInteractive {
		pending = &s.interactivePendingSyncs
	}
	for len(*pending) > 0 {
		key := (*pending)[0]
		*pending = (*pending)[1:]
		task, ok := s.partitionSyncs[key]
		if !ok || task == nil || task.started || task.queue != queue {
			continue
		}
		return task
	}
	return nil
}

func (s *Service) enqueuePartitionSyncLocked(task *partitionSyncTask) {
	if task == nil {
		return
	}
	if task.queue == partitionSyncQueueInteractive {
		s.prependInteractivePendingLocked(task.key)
		return
	}
	s.maintenancePendingSyncs = append(s.maintenancePendingSyncs, task.key)
}

func (s *Service) prependInteractivePendingLocked(key string) {
	if key == "" {
		return
	}
	s.interactivePendingSyncs = append([]string{key}, s.interactivePendingSyncs...)
}

func (s *Service) tryStartInteractiveUrgentLocked(task *partitionSyncTask) bool {
	if task == nil || task.started || task.queue != partitionSyncQueueInteractive {
		return false
	}
	if s.interactiveUrgentSem == nil {
		return false
	}
	select {
	case s.interactiveUrgentSem <- struct{}{}:
		task.started = true
		task.startedAt = time.Now().UTC()
		go s.executePartitionSyncTask(task, -1, true, func() {
			<-s.interactiveUrgentSem
		})
		return true
	default:
		return false
	}
}

func (s *Service) executePartitionSyncTask(task *partitionSyncTask, workerID int, urgent bool, release func()) {
	if task == nil {
		if release != nil {
			release()
		}
		return
	}
	if release != nil {
		defer release()
	}

	fields := []zap.Field{
		zap.String("reason", task.reason),
		zap.String("queue", string(task.queue)),
		zap.String("datasource", task.datasource.Name),
		zap.String("service", displayServiceName(task.serviceName)),
		zap.String("day", task.day.Format("2006-01-02")),
	}
	if urgent {
		fields = append(fields, zap.Bool("urgent", true))
	} else {
		fields = append(fields, zap.Int("worker_id", workerID))
	}
	s.logger.Info("partition sync started", fields...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	err := func() (runErr error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				runErr = fmt.Errorf("partition sync panic: %v", recovered)
			}
		}()
		return task.runner(ctx)
	}()
	cancel()
	s.completePartitionSyncTask(task, err)
}

func (s *Service) completePartitionSyncTask(task *partitionSyncTask, err error) {
	if task == nil {
		return
	}
	task.err = err
	if err != nil {
		s.logger.Warn("partition sync failed",
			zap.Error(err),
			zap.String("reason", task.reason),
			zap.String("queue", string(task.queue)),
			zap.String("datasource", task.datasource.Name),
			zap.String("service", displayServiceName(task.serviceName)),
			zap.String("day", task.day.Format("2006-01-02")),
			zap.Duration("took", time.Since(task.startedAt)),
		)
	} else {
		s.logger.Info("partition sync completed",
			zap.String("reason", task.reason),
			zap.String("queue", string(task.queue)),
			zap.String("datasource", task.datasource.Name),
			zap.String("service", displayServiceName(task.serviceName)),
			zap.String("day", task.day.Format("2006-01-02")),
			zap.Duration("took", time.Since(task.startedAt)),
		)
	}

	s.partitionSyncMu.Lock()
	delete(s.partitionSyncs, task.key)
	close(task.done)
	s.partitionSyncMu.Unlock()
}

func (s *Service) servicePriorityKey(datasourceID, service string) string {
	return datasourceID + ":" + service
}

func (s *Service) markServiceInteractive(datasource model.Datasource, serviceName string, now time.Time) {
	if strings.TrimSpace(serviceName) == "" {
		return
	}
	ttl := s.cfg.InteractiveServiceTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	key := s.servicePriorityKey(datasource.ID, serviceName)
	s.servicePriorityMu.Lock()
	s.servicePriorityTTL[key] = now.UTC().Add(ttl)
	s.cleanupExpiredServicePriorityLocked(now.UTC())
	s.servicePriorityMu.Unlock()
}

func (s *Service) preferredSyncQueue(datasource model.Datasource, serviceName string, now time.Time, fallback partitionSyncQueue) partitionSyncQueue {
	if strings.TrimSpace(serviceName) == "" {
		return fallback
	}
	key := s.servicePriorityKey(datasource.ID, serviceName)
	s.servicePriorityMu.Lock()
	defer s.servicePriorityMu.Unlock()
	s.cleanupExpiredServicePriorityLocked(now.UTC())
	expireAt, ok := s.servicePriorityTTL[key]
	if ok && expireAt.After(now.UTC()) {
		return partitionSyncQueueInteractive
	}
	return fallback
}

func (s *Service) cleanupExpiredServicePriorityLocked(now time.Time) {
	for key, expireAt := range s.servicePriorityTTL {
		if !expireAt.After(now) {
			delete(s.servicePriorityTTL, key)
		}
	}
}
