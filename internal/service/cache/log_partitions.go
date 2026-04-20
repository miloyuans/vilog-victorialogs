package cache

import (
	"bufio"
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
	logPartitionRowsFile      = "rows.json"
	logPartitionMetaFile      = "meta.json"
	logPartitionTextFile      = "logs.txt.gz"
	logPartitionCompleteFile  = "complete.marker"
	logPartitionFormatVersion = 2
)

type LocalLogPartition struct {
	Meta LocalLogPartitionMeta `json:"meta"`
	Rows []model.SearchResult  `json:"rows"`
}

type LocalLogPartitionMeta struct {
	FormatVersion  int       `json:"format_version"`
	Date           string    `json:"date"`
	Service        string    `json:"service"`
	DatasourceID   string    `json:"datasource_id"`
	DatasourceName string    `json:"datasource_name"`
	LastSyncAt     time.Time `json:"last_sync_at"`
	LastAccessAt   time.Time `json:"last_access_at"`
	ExpireAt       time.Time `json:"expire_at"`
	RowCount       int       `json:"row_count"`
	Partial        bool      `json:"partial"`
	Building       bool      `json:"building"`
}

type logPartitionCompleteMarker struct {
	FormatVersion int       `json:"format_version"`
	CompletedAt   time.Time `json:"completed_at"`
}

type storedSearchResult struct {
	Timestamp  string            `json:"timestamp"`
	Message    string            `json:"message"`
	Service    string            `json:"service"`
	Pod        string            `json:"pod"`
	Datasource string            `json:"datasource"`
	Labels     map[string]string `json:"labels,omitempty"`
}

func (s *Service) LoadLogPartition(day time.Time, service string, datasource model.Datasource) (LocalLogPartition, bool, error) {
	if strings.TrimSpace(s.cfg.LocalLogDir) == "" {
		return LocalLogPartition{}, false, nil
	}
	s.maybeCleanupLocalLogPartitions()

	dir := s.logPartitionDir(day, service, datasource)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LocalLogPartition{}, false, nil
		}
		return LocalLogPartition{}, false, err
	}

	metaPath := filepath.Join(dir, logPartitionMetaFile)
	rowsPath := filepath.Join(dir, logPartitionRowsFile)
	completePath := filepath.Join(dir, logPartitionCompleteFile)

	marker, err := s.readLogPartitionMarker(completePath)
	if err != nil {
		s.discardBrokenLogPartition(dir, logPartitionCompleteFile, err, datasource.Name, service, s.partitionDate(day))
		return LocalLogPartition{}, false, nil
	}
	if marker.FormatVersion != logPartitionFormatVersion {
		s.discardBrokenLogPartition(dir, logPartitionCompleteFile, fmt.Errorf("unsupported marker format version %d", marker.FormatVersion), datasource.Name, service, s.partitionDate(day))
		return LocalLogPartition{}, false, nil
	}

	meta, err := s.readLogPartitionMeta(metaPath)
	if err != nil {
		s.discardBrokenLogPartition(dir, logPartitionMetaFile, err, datasource.Name, service, s.partitionDate(day))
		return LocalLogPartition{}, false, nil
	}
	if meta.FormatVersion != logPartitionFormatVersion {
		s.discardBrokenLogPartition(dir, logPartitionMetaFile, fmt.Errorf("unsupported partition format version %d", meta.FormatVersion), datasource.Name, service, meta.Date)
		return LocalLogPartition{}, false, nil
	}

	now := time.Now().UTC()
	if !meta.ExpireAt.IsZero() && now.After(meta.ExpireAt) && !s.isHotLogDay(day, now) {
		_ = os.RemoveAll(dir)
		return LocalLogPartition{}, false, nil
	}

	rows, err := s.readLogPartitionRows(rowsPath)
	if err != nil {
		s.discardBrokenLogPartition(dir, logPartitionRowsFile, err, datasource.Name, service, meta.Date)
		return LocalLogPartition{}, false, nil
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
	completePath := filepath.Join(dir, logPartitionCompleteFile)

	partition.Meta.FormatVersion = logPartitionFormatVersion
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

	_ = os.Remove(completePath)
	if err := s.writeRowsJSONAtomic(rowsPath, partition.Rows); err != nil {
		return err
	}
	if err := s.writeJSONAtomic(metaPath, partition.Meta); err != nil {
		return err
	}
	if err := s.writeGzipRowsAtomic(textPath, partition.Rows); err != nil {
		return err
	}
	if err := s.writeJSONAtomic(completePath, logPartitionCompleteMarker{
		FormatVersion: partition.Meta.FormatVersion,
		CompletedAt:   time.Now().UTC(),
	}); err != nil {
		return err
	}

	s.logger.Debug("stored log partition",
		zap.String("datasource", partition.Meta.DatasourceName),
		zap.String("service", defaultPartitionService(partition.Meta.Service)),
		zap.String("date", partition.Meta.Date),
		zap.Int("rows", len(partition.Rows)),
		zap.Bool("partial", partition.Meta.Partial),
		zap.Bool("building", partition.Meta.Building),
	)
	return nil
}

func (s *Service) PrepareLogPartitionMeta(day time.Time, service string, datasource model.Datasource, rowCount int, now time.Time, partial bool) LocalLogPartitionMeta {
	meta := LocalLogPartitionMeta{
		FormatVersion:  logPartitionFormatVersion,
		Date:           s.partitionDate(day),
		Service:        service,
		DatasourceID:   datasource.ID,
		DatasourceName: datasource.Name,
		LastSyncAt:     now.UTC(),
		LastAccessAt:   now.UTC(),
		RowCount:       rowCount,
		Partial:        partial,
		Building:       false,
	}
	if !s.isHotLogDay(day, now) {
		meta.ExpireAt = now.UTC().Add(s.cfg.LocalLogHistoryTTL)
	}
	return meta
}

func (s *Service) PreparePendingLogPartitionMeta(day time.Time, service string, datasource model.Datasource, rowCount int, now time.Time) LocalLogPartitionMeta {
	meta := s.PrepareLogPartitionMeta(day, service, datasource, rowCount, now, true)
	meta.Building = true
	return meta
}

func (s *Service) LogPartitionNeedsRefresh(meta LocalLogPartitionMeta, day, now time.Time) bool {
	if meta.Building {
		return false
	}
	if !truncateToUTCDay(day).Equal(truncateToUTCDay(now)) {
		return false
	}
	if meta.LastSyncAt.IsZero() {
		return true
	}
	interval := s.cfg.LocalLogRefreshInterval
	if interval <= 0 {
		interval = 60 * time.Second
	}
	if meta.Partial {
		interval = interval * 5
		if interval < 5*time.Minute {
			interval = 5 * time.Minute
		}
	}
	return now.UTC().Sub(meta.LastSyncAt.UTC()) >= interval
}

func (s *Service) LogPartitionBuildStale(meta LocalLogPartitionMeta, now time.Time) bool {
	if !meta.Building {
		return false
	}
	if meta.LastSyncAt.IsZero() {
		return true
	}
	lease := s.cfg.LocalLogRefreshInterval
	if lease <= 0 || lease < 30*time.Second {
		lease = 30 * time.Second
	}
	return now.UTC().Sub(meta.LastSyncAt.UTC()) >= lease
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
		dir := filepath.Dir(path)
		meta, err := s.readLogPartitionMeta(path)
		if err != nil {
			s.discardBrokenLogPartition(dir, logPartitionMetaFile, err, "", "", "")
			return nil
		}
		if meta.FormatVersion != logPartitionFormatVersion {
			s.discardBrokenLogPartition(dir, logPartitionMetaFile, fmt.Errorf("unsupported partition format version %d", meta.FormatVersion), meta.DatasourceName, meta.Service, meta.Date)
			return nil
		}
		if _, err := s.readLogPartitionMarker(filepath.Join(dir, logPartitionCompleteFile)); err != nil {
			s.discardBrokenLogPartition(dir, logPartitionCompleteFile, err, meta.DatasourceName, meta.Service, meta.Date)
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
		dir := filepath.Dir(path)
		meta, readErr := s.readLogPartitionMeta(path)
		if readErr != nil {
			s.discardBrokenLogPartition(dir, logPartitionMetaFile, readErr, "", "", "")
			return nil
		}
		if meta.FormatVersion != logPartitionFormatVersion {
			s.discardBrokenLogPartition(dir, logPartitionMetaFile, fmt.Errorf("unsupported partition format version %d", meta.FormatVersion), meta.DatasourceName, meta.Service, meta.Date)
			return nil
		}
		if _, markerErr := s.readLogPartitionMarker(filepath.Join(dir, logPartitionCompleteFile)); markerErr != nil {
			s.discardBrokenLogPartition(dir, logPartitionCompleteFile, markerErr, meta.DatasourceName, meta.Service, meta.Date)
			return nil
		}
		day, dayErr := time.Parse("2006-01-02", meta.Date)
		if dayErr != nil {
			s.discardBrokenLogPartition(dir, logPartitionMetaFile, dayErr, meta.DatasourceName, meta.Service, meta.Date)
			return nil
		}
		now := time.Now().UTC()
		if !meta.ExpireAt.IsZero() && now.After(meta.ExpireAt) && !s.isHotLogDay(day, now) {
			s.logger.Info("removing expired log partition",
				zap.String("datasource", meta.DatasourceName),
				zap.String("service", defaultPartitionService(meta.Service)),
				zap.String("date", meta.Date),
				zap.String("path", dir),
			)
			_ = os.RemoveAll(dir)
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

func (s *Service) readLogPartitionMarker(path string) (logPartitionCompleteMarker, error) {
	var marker logPartitionCompleteMarker
	raw, err := os.ReadFile(path)
	if err != nil {
		return logPartitionCompleteMarker{}, err
	}
	if err := json.Unmarshal(raw, &marker); err != nil {
		return logPartitionCompleteMarker{}, err
	}
	return marker, nil
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

func (s *Service) writeRowsJSONAtomic(path string, rows []model.SearchResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	cleanup := func(writeErr error) error {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return writeErr
	}

	writer := bufio.NewWriterSize(file, 64*1024)
	if _, err := writer.WriteString("["); err != nil {
		return cleanup(err)
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for index, row := range rows {
		if index > 0 {
			if _, err := writer.WriteString(","); err != nil {
				return cleanup(err)
			}
		}
		if err := encoder.Encode(storedSearchResult{
			Timestamp:  row.Timestamp,
			Message:    row.Message,
			Service:    row.Service,
			Pod:        row.Pod,
			Datasource: row.Datasource,
			Labels:     row.Labels,
		}); err != nil {
			return cleanup(err)
		}
	}
	if _, err := writer.WriteString("]"); err != nil {
		return cleanup(err)
	}
	if err := writer.Flush(); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tempPath, path)
}

func (s *Service) writeGzipRowsAtomic(path string, rows []model.SearchResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	cleanup := func(writeErr error) error {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return writeErr
	}

	gzipWriter := gzip.NewWriter(file)
	writer := bufio.NewWriterSize(gzipWriter, 64*1024)
	for index, row := range rows {
		if index > 0 {
			if _, err := writer.WriteString("\n"); err != nil {
				_ = gzipWriter.Close()
				return cleanup(err)
			}
		}
		if _, err := writer.WriteString(formatPartitionLine(row)); err != nil {
			_ = gzipWriter.Close()
			return cleanup(err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = gzipWriter.Close()
		return cleanup(err)
	}
	if err := gzipWriter.Close(); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tempPath, path)
}

func (s *Service) discardBrokenLogPartition(dir, file string, reason error, datasourceName, serviceName, date string) {
	fields := []zap.Field{
		zap.String("path", dir),
		zap.String("file", file),
		zap.String("action", "delete"),
	}
	if strings.TrimSpace(datasourceName) != "" {
		fields = append(fields, zap.String("datasource", datasourceName))
	}
	if strings.TrimSpace(serviceName) != "" {
		fields = append(fields, zap.String("service", defaultPartitionService(serviceName)))
	}
	if strings.TrimSpace(date) != "" {
		fields = append(fields, zap.String("date", date))
	}
	if reason != nil {
		fields = append(fields, zap.Error(reason))
	}
	s.logger.Warn("dropping broken log partition", fields...)
	if err := os.RemoveAll(dir); err != nil {
		s.logger.Warn("delete broken log partition failed",
			zap.String("path", dir),
			zap.Error(err),
		)
	}
}

func formatPartitionLine(row model.SearchResult) string {
	pod := strings.TrimSpace(row.Pod)
	if pod == "" {
		pod = strings.TrimSpace(row.Service)
	}
	return fmt.Sprintf("[%s] %s %s %s", row.Timestamp, row.Datasource, pod, row.Message)
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
