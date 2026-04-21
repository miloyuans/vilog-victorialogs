package localstore

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

	"vilog-victorialogs/internal/model"
)

type QuerySegmentStore struct {
	baseDir string
}

type SegmentMeta struct {
	FilePath      string
	RowCount      int64
	SizeBytes     int64
	TimeMin       *time.Time
	TimeMax       *time.Time
	DatasourceIDs []string
}

type SegmentWriter struct {
	file          *os.File
	gzipWriter    *gzip.Writer
	buffered      *bufio.Writer
	path          string
	rowCount      int64
	sizeBytes     int64
	timeMin       *time.Time
	timeMax       *time.Time
	datasourceIDs map[string]struct{}
}

func NewQuerySegmentStore(baseDir string) *QuerySegmentStore {
	return &QuerySegmentStore{baseDir: strings.TrimSpace(baseDir)}
}

func (s *QuerySegmentStore) EnsureJobDir(jobID string) (string, error) {
	if s == nil || strings.TrimSpace(s.baseDir) == "" {
		return "", fmt.Errorf("query segment store base dir is empty")
	}
	dir := filepath.Join(s.baseDir, jobID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir query job dir: %w", err)
	}
	return dir, nil
}

func (s *QuerySegmentStore) DeleteJob(jobID string) error {
	if s == nil || strings.TrimSpace(s.baseDir) == "" {
		return nil
	}
	target := filepath.Join(s.baseDir, jobID)
	if err := os.RemoveAll(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove query job dir: %w", err)
	}
	return nil
}

func (s *QuerySegmentStore) OpenWriter(jobID string, sequence int64) (*SegmentWriter, error) {
	dir, err := s.EnsureJobDir(jobID)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("segment_%06d.ndjson.gz", sequence))
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create segment file: %w", err)
	}
	gz := gzip.NewWriter(file)
	return &SegmentWriter{
		file:          file,
		gzipWriter:    gz,
		buffered:      bufio.NewWriterSize(gz, 256*1024),
		path:          path,
		datasourceIDs: map[string]struct{}{},
	}, nil
}

func (w *SegmentWriter) Write(row model.SearchResult) error {
	if w == nil || w.buffered == nil {
		return fmt.Errorf("segment writer is not ready")
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshal segment row: %w", err)
	}
	payload = append(payload, '\n')
	n, err := w.buffered.Write(payload)
	if err != nil {
		return fmt.Errorf("write segment row: %w", err)
	}
	w.rowCount++
	w.sizeBytes += int64(n)
	if strings.TrimSpace(row.Datasource) != "" {
		w.datasourceIDs[row.Datasource] = struct{}{}
	}
	if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(row.Timestamp)); err == nil {
		if w.timeMin == nil || ts.Before(*w.timeMin) {
			copy := ts
			w.timeMin = &copy
		}
		if w.timeMax == nil || ts.After(*w.timeMax) {
			copy := ts
			w.timeMax = &copy
		}
	}
	return nil
}

func (w *SegmentWriter) Close() (SegmentMeta, error) {
	if w == nil || w.file == nil {
		return SegmentMeta{}, fmt.Errorf("segment writer is not open")
	}
	var firstErr error
	if err := w.buffered.Flush(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.gzipWriter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	datasourceIDs := make([]string, 0, len(w.datasourceIDs))
	for id := range w.datasourceIDs {
		datasourceIDs = append(datasourceIDs, id)
	}
	if firstErr != nil {
		return SegmentMeta{}, fmt.Errorf("close segment writer: %w", firstErr)
	}
	info, err := os.Stat(w.path)
	if err != nil {
		return SegmentMeta{}, fmt.Errorf("stat segment file: %w", err)
	}
	return SegmentMeta{
		FilePath:      w.path,
		RowCount:      w.rowCount,
		SizeBytes:     info.Size(),
		TimeMin:       w.timeMin,
		TimeMax:       w.timeMax,
		DatasourceIDs: datasourceIDs,
	}, nil
}

func (s *QuerySegmentStore) ReadRows(path string, visit func(model.SearchResult) (bool, error)) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open segment file: %w", err)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("open segment gzip reader: %w", err)
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 256*1024), 16*1024*1024)
	for scanner.Scan() {
		var row model.SearchResult
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return fmt.Errorf("decode segment row: %w", err)
		}
		cont, err := visit(row)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan segment rows: %w", err)
	}
	return nil
}
