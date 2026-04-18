package cache

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
)

func TestLoadLogPartitionSelfHealsBrokenFiles(t *testing.T) {
	svc := newTestCacheService(t)
	day := time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC)
	datasource := model.Datasource{ID: "ds-1", Name: "primary"}
	partition := LocalLogPartition{
		Meta: svc.PrepareLogPartitionMeta(day, "payments", datasource, 1, time.Now().UTC(), false),
		Rows: []model.SearchResult{
			{
				Timestamp:  "2026-04-18T10:00:00Z",
				Message:    "wallet failed",
				Service:    "payments",
				Pod:        "payments-0",
				Datasource: datasource.Name,
			},
		},
	}
	if err := svc.StoreLogPartition(partition); err != nil {
		t.Fatalf("StoreLogPartition() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(dir string)
	}{
		{
			name: "corrupt meta",
			mutate: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, logPartitionMetaFile), []byte("{"), 0o644)
			},
		},
		{
			name: "corrupt rows",
			mutate: func(dir string) {
				_ = os.WriteFile(filepath.Join(dir, logPartitionRowsFile), []byte("{"), 0o644)
			},
		},
		{
			name: "missing marker",
			mutate: func(dir string) {
				_ = os.Remove(filepath.Join(dir, logPartitionCompleteFile))
			},
		},
		{
			name: "version mismatch",
			mutate: func(dir string) {
				metaPath := filepath.Join(dir, logPartitionMetaFile)
				raw, _ := os.ReadFile(metaPath)
				var meta LocalLogPartitionMeta
				_ = json.Unmarshal(raw, &meta)
				meta.FormatVersion = logPartitionFormatVersion + 99
				updated, _ := json.Marshal(meta)
				_ = os.WriteFile(metaPath, updated, 0o644)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := svc.logPartitionDir(day, "payments", datasource)
			if err := svc.StoreLogPartition(partition); err != nil {
				t.Fatalf("StoreLogPartition() error = %v", err)
			}
			tt.mutate(dir)

			loaded, hit, err := svc.LoadLogPartition(day, "payments", datasource)
			if err != nil {
				t.Fatalf("LoadLogPartition() error = %v", err)
			}
			if hit {
				t.Fatalf("LoadLogPartition() hit = true, loaded = %+v", loaded)
			}
			if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
				t.Fatalf("expected broken partition directory to be removed, stat err = %v", statErr)
			}
		})
	}
}

func TestStoreLogPartitionWritesReadableGzipText(t *testing.T) {
	svc := newTestCacheService(t)
	day := time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC)
	datasource := model.Datasource{ID: "ds-2", Name: "secondary"}
	partition := LocalLogPartition{
		Meta: svc.PrepareLogPartitionMeta(day, "finance", datasource, 2, time.Now().UTC(), false),
		Rows: []model.SearchResult{
			{
				Timestamp:  "2026-04-18T10:00:00Z",
				Message:    "line one",
				Service:    "finance",
				Pod:        "finance-0",
				Datasource: datasource.Name,
			},
			{
				Timestamp:  "2026-04-18T10:00:01Z",
				Message:    "line two",
				Service:    "finance",
				Pod:        "finance-0",
				Datasource: datasource.Name,
			},
		},
	}
	if err := svc.StoreLogPartition(partition); err != nil {
		t.Fatalf("StoreLogPartition() error = %v", err)
	}

	textPath := filepath.Join(svc.logPartitionDir(day, "finance", datasource), logPartitionTextFile)
	file, err := os.Open(textPath)
	if err != nil {
		t.Fatalf("Open(%s) error = %v", textPath, err)
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	text := string(raw)
	if !strings.Contains(text, "[2026-04-18T10:00:00Z] secondary finance-0 line one") {
		t.Fatalf("gzip text missing first line: %s", text)
	}
	if !strings.Contains(text, "[2026-04-18T10:00:01Z] secondary finance-0 line two") {
		t.Fatalf("gzip text missing second line: %s", text)
	}
}

func newTestCacheService(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	return New(nil, config.CacheConfig{
		LocalLogDir:         dir,
		LocalLogHotDays:     2,
		LocalLogHistoryTTL:  time.Hour,
		LocalQueryRetention: time.Hour,
	}, zap.NewNop())
}
