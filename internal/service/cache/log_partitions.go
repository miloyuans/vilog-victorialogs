package cache

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/model"
)

const (
	logPartitionRowsFile = "rows.json"
	logPartitionMetaFile = "meta.json"
	logPartitionTextFile = "logs.txt.gz"
)

type LocalLogPartition struct {
	Meta LocalLogPartitionMeta `json:"meta"`
	Rows []model.SearchResult  `json:"rows"`
}

type LocalLogPartitionMeta struct {
	Date           string    `json:"date"`
	Service        string    `json:"service"`
	DatasourceID   string    `json:"datasource_id"`
	DatasourceName string    `json:"datasource_name"`
	LastSyncAt     time.Time `json:"last_sync_at"`
	LastAccessAt   time.Time `json:"last_access_at"`
	ExpireAt       time.Time `json:"expire_at"`
	RowCount       int       `json:"row_count"`
	Partial        bool      `json:"partial"`
}

func (s *Service) LoadLogPartition(day time.Time, service string, datasource model.Datasource) (LocalLogPartition, bool, error) {
	if strings.TrimSpace(s.cfg.LocalLogDir) == "" {
		return LocalLogPartition{}, false, nil
	}
	s.maybeCleanupLocalLogPartitions()

	dir := s.logPartitionDir(day, service, datasource)
	metaPath := filepath.Join(dir, logPartitionMetaFile)
	rowsPath := filepath.Join(dir, logPartitionRowsFile)

	meta, err := s.readLogPartitionMeta(metaPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LocalLogPartition{}, false, nil
		}
		return LocalLogPartition{}, false, err
	}

	now := time.Now().UTC()
	if !meta.ExpireAt.IsZero() && now.After(meta.ExpireAt) && !s.isHotLogDay(day, now) {
		_ = os.RemoveAll(dir)
		return LocalLogPartition{}, false, nil
	}

	rows, err := s.readLogPartitionRows(rowsPath)
	if err != nil {
		_ = os.RemoveAll(dir)
		return LocalLogPartition{}, false, err
	}

	meta.LastAccessAt = now
	if !s.isHotLogDay(day, now) {
		meta.ExpireAt = now.Add(s.cfg.LocalLogHistoryTTL)
	}
	if err := s.writeJSONAtomic(metaPath, meta); err != nil {
		s.logger.Warn("touch log partition meta failed", zap.Error(err), zap.String("path", metaPath))
	}
	s.logger.Debug("loaded log partition",
		zap.String("datasource", datasource.Name),
		zap.String("service", defaultPartitionService(service)),
		zap.String("date", meta.Date),
		zap.Int("rows", len(rows)),
	)

	return LocalLogPartition{
		Meta: meta,
		Rows: rows,
	}, true, nil
}

func (s *Service) StoreLogPartition(partition LocalLogPartition) error {
	if strings.TrimSpace(s.cfg.LocalLogDir) == "" {
		return nil
	}
	s.maybeCleanupLocalLogPartitions()

	day, err := time.Parse("2006-01-02", partition.Meta.Date)
	if err != nil {
		return fmt.Errorf("parse log partition date: %w", err)
	}
	datasource := model.Datasource{
		ID:   partition.Meta.DatasourceID,
		Name: partition.Meta.DatasourceName,
	}
	dir := s.logPartitionDir(day, partition.Meta.Service, datasource)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	rowsPath := filepath.Join(dir, logPartitionRowsFile)
	metaPath := filepath.Join(dir, logPartitionMetaFile)
	textPath := filepath.Join(dir, logPartitionTextFile)

	partition.Meta.RowCount = len(partition.Rows)
	if partition.Meta.LastAccessAt.IsZero() {
		partition.Meta.LastAccessAt = time.Now().UTC()
	}
	if partition.Meta.LastSyncAt.IsZero() {
		partition.Meta.LastSyncAt = partition.Meta.LastAccessAt
	}
	if partition.Meta.ExpireAt.IsZero() && !s.isHotLogDay(day, partition.Meta.LastAccessAt) {
		partition.Meta.ExpireAt = partition.Meta.LastAccessAt.Add(s.cfg.LocalLogHistoryTTL)
	}

	storableRows := prepareRowsForLocalStorage(partition.Rows)
	if err := s.writeJSONAtomic(rowsPath, storableRows); err != nil {
		return err
	}
	if err := s.writeJSONAtomic(metaPath, partition.Meta); err != nil {
		return err
	}
	if err := s.writeGzipTextAtomic(textPath, s.renderPartitionText(storableRows)); err != nil {
		return err
	}
	s.logger.Debug("stored log partition",
		zap.String("datasource", partition.Meta.DatasourceName),
		zap.String("service", defaultPartitionService(partition.Meta.Service)),
		zap.String("date", partition.Meta.Date),
		zap.Int("rows", len(storableRows)),
	)
	return nil
}

func (s *Service) PrepareLogPartitionMeta(day time.Time, service string, datasource model.Datasource, rowCount int, now time.Time, partial bool) LocalLogPartitionMeta {
	meta := LocalLogPartitionMeta{
		Date:           s.partitionDate(day),
		Service:        service,
		DatasourceID:   datasource.ID,
		DatasourceName: datasource.Name,
		LastSyncAt:     now.UTC(),
		LastAccessAt:   now.UTC(),
		RowCount:       rowCount,
		Partial:        partial,
	}
	if !s.isHotLogDay(day, now) {
		meta.ExpireAt = now.UTC().Add(s.cfg.LocalLogHistoryTTL)
	}
	return meta
}

func (s *Service) LogPartitionNeedsRefresh(meta LocalLogPartitionMeta, day, now time.Time) bool {
	if !truncateToUTCDay(day).Equal(truncateToUTCDay(now)) {
		return false
	}
	if meta.LastSyncAt.IsZero() {
		return true
	}
	return now.UTC().Sub(meta.LastSyncAt.UTC()) >= s.cfg.LocalLogRefreshInterval
}

func (s *Service) ListTrackedLogPartitions() ([]LocalLogPartitionMeta, error) {
	baseDir := strings.TrimSpace(s.cfg.LocalLogDir)
	if baseDir == "" {
		return nil, nil
	}
	s.maybeCleanupLocalLogPartitions()

	metas := make([]LocalLogPartitionMeta, 0)
	err := filepath.WalkDir(baseDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() || d.Name() != logPartitionMetaFile {
			return nil
		}
		meta, readErr := s.readLogPartitionMeta(path)
		if readErr != nil {
			s.logger.Warn("read tracked log partition meta failed", zap.Error(readErr), zap.String("path", path))
			return nil
		}
		metas = append(metas, meta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return metas, nil
}

func (s *Service) maybeCleanupLocalLogPartitions() {
	baseDir := strings.TrimSpace(s.cfg.LocalLogDir)
	if baseDir == "" {
		return
	}

	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()

	if time.Since(s.lastLogDirCleanup) < 5*time.Minute {
		return
	}
	s.lastLogDirCleanup = time.Now()

	_ = filepath.WalkDir(baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || d.Name() != logPartitionMetaFile {
			return nil
		}
		meta, readErr := s.readLogPartitionMeta(path)
		if readErr != nil {
			_ = os.RemoveAll(filepath.Dir(path))
			return nil
		}
		day, dayErr := time.Parse("2006-01-02", meta.Date)
		if dayErr != nil {
			_ = os.RemoveAll(filepath.Dir(path))
			return nil
		}
		now := time.Now().UTC()
		if !meta.ExpireAt.IsZero() && now.After(meta.ExpireAt) && !s.isHotLogDay(day, now) {
			s.logger.Info("removing expired log partition",
				zap.String("datasource", meta.DatasourceName),
				zap.String("service", defaultPartitionService(meta.Service)),
				zap.String("date", meta.Date),
				zap.String("path", filepath.Dir(path)),
			)
			_ = os.RemoveAll(filepath.Dir(path))
		}
		return nil
	})
}

func (s *Service) logPartitionDir(day time.Time, service string, datasource model.Datasource) string {
	return filepath.Join(
		strings.TrimSpace(s.cfg.LocalLogDir),
		s.partitionDate(day),
		sanitizeLogPathSegment(defaultPartitionService(service)),
		sanitizeLogPathSegment(datasource.Name+"__"+datasource.ID),
	)
}

func (s *Service) readLogPartitionMeta(path string) (LocalLogPartitionMeta, error) {
	var meta LocalLogPartitionMeta
	raw, err := os.ReadFile(path)
	if err != nil {
		return LocalLogPartitionMeta{}, err
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return LocalLogPartitionMeta{}, err
	}
	return meta, nil
}

func (s *Service) readLogPartitionRows(path string) ([]model.SearchResult, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	rows := make([]model.SearchResult, 0)
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Service) writeJSONAtomic(path string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, raw, 0o644); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tempPath, path)
}

func (s *Service) writeGzipTextAtomic(path, text string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	writer := gzip.NewWriter(file)
	if _, err := writer.Write([]byte(text)); err != nil {
		_ = writer.Close()
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := writer.Close(); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tempPath, path)
}

func (s *Service) renderPartitionText(rows []model.SearchResult) string {
	if len(rows) == 0 {
		return ""
	}
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		pod := strings.TrimSpace(row.Pod)
		if pod == "" {
			pod = strings.TrimSpace(row.Service)
		}
		lines = append(lines, fmt.Sprintf("[%s] %s %s %s", row.Timestamp, row.Datasource, pod, row.Message))
	}
	return strings.Join(lines, "\n")
}

func (s *Service) isHotLogDay(day, now time.Time) bool {
	dayUTC := truncateToUTCDay(day)
	nowUTC := truncateToUTCDay(now)
	if dayUTC.After(nowUTC) {
		return false
	}
	ageDays := int(nowUTC.Sub(dayUTC) / (24 * time.Hour))
	return ageDays >= 0 && ageDays < s.cfg.LocalLogHotDays
}

func (s *Service) partitionDate(day time.Time) string {
	return truncateToUTCDay(day).Format("2006-01-02")
}

func truncateToUTCDay(day time.Time) time.Time {
	utc := day.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

func defaultPartitionService(service string) string {
	if strings.TrimSpace(service) == "" {
		return "__all__"
	}
	return service
}

func sanitizeLogPathSegment(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "_"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r < 32:
			return '_'
		case strings.ContainsRune(`<>:"/\|?*`, r):
			return '_'
		default:
			return r
		}
	}, trimmed)
}

func prepareRowsForLocalStorage(rows []model.SearchResult) []model.SearchResult {
	if len(rows) == 0 {
		return nil
	}
	compact := make([]model.SearchResult, 0, len(rows))
	for _, row := range rows {
		clone := row
		clone.Raw = nil
		compact = append(compact, clone)
	}
	return compact
}
