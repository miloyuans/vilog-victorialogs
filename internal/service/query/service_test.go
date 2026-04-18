package query

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
)

func TestStartPartitionSyncDeduplicatesAndLimitsMaintenanceQueue(t *testing.T) {
	svc := newTestQueryService(config.CacheConfig{
		MaxPendingPartitionSyncs: 1,
	})
	day := time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC)
	datasource := model.Datasource{ID: "ds", Name: "primary"}

	first, created := svc.startPartitionSync(day, datasource, "svc-a", "hot-build", partitionSyncQueueMaintenance, func(context.Context) error {
		return nil
	})
	if !created || first == nil {
		t.Fatalf("first maintenance task was not created")
	}

	second, created := svc.startPartitionSync(day, datasource, "svc-b", "hot-build", partitionSyncQueueMaintenance, func(context.Context) error {
		return nil
	})
	if created || second != nil {
		t.Fatalf("expected second maintenance task to be rejected when queue is full")
	}

	duplicate, created := svc.startPartitionSync(day, datasource, "svc-a", "query-build", partitionSyncQueueInteractive, func(context.Context) error {
		return nil
	})
	if created || duplicate != first {
		t.Fatalf("expected duplicate task to reuse existing task")
	}
	if got := len(svc.maintenancePendingSyncs); got != 1 {
		t.Fatalf("maintenance pending queue len = %d, want 1", got)
	}
}

func TestNormalizeRowsForPartitionAppliesSafetyGuards(t *testing.T) {
	svc := newTestQueryService(config.CacheConfig{
		MaxPartitionRows:     4,
		MaxRowsBeforePartial: 4,
		MaxDedupeRows:        4,
		MaxSortRows:          4,
	})
	datasource := model.Datasource{Name: "primary"}
	rows := []model.SearchResult{
		{Timestamp: "2026-04-18T10:00:04Z", Message: "four", Datasource: "primary"},
		{Timestamp: "2026-04-18T10:00:03Z", Message: "three", Datasource: "primary"},
		{Timestamp: "2026-04-18T10:00:02Z", Message: "two", Datasource: "primary"},
		{Timestamp: "2026-04-18T10:00:01Z", Message: "one", Datasource: "primary"},
		{Timestamp: "2026-04-18T10:00:00Z", Message: "zero", Datasource: "primary"},
	}

	filtered, partial := svc.normalizeRowsForPartition(rows, datasource, "payments", time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC), "test")
	if !partial {
		t.Fatalf("expected partial=true when rows exceed safety limits")
	}
	if got := len(filtered); got != 4 {
		t.Fatalf("len(filtered) = %d, want 4", got)
	}
}

func newTestQueryService(cfg config.CacheConfig) *Service {
	svc := &Service{
		cfg:              cfg,
		logger:           zap.NewNop(),
		partitionSyncs:   make(map[string]*partitionSyncTask),
		servicePriorityTTL: make(map[string]time.Time),
	}
	svc.partitionSyncCond = sync.NewCond(&svc.partitionSyncMu)
	return svc
}
