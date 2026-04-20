package cache

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

const (
	KindQuery       = "query"
	KindServiceList = "service_list"
	KindTagValues   = "tag_values"

	maxMongoQueryCacheBytes = 8 << 20
)

type Service struct {
	store              *mongostore.Store
	cfg                config.CacheConfig
	logger             *zap.Logger
	cleanupMu          sync.Mutex
	lastLocalCleanup   time.Time
	lastLogDirCleanup  time.Time
}

func New(store *mongostore.Store, cfg config.CacheConfig, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	service := &Service{
		store: store,
		cfg:   cfg,
		logger: logger,
	}
	if strings.TrimSpace(cfg.LocalQueryDir) != "" {
		_ = os.MkdirAll(cfg.LocalQueryDir, 0o755)
	}
	if strings.TrimSpace(cfg.LocalLogDir) != "" {
		_ = os.MkdirAll(cfg.LocalLogDir, 0o755)
	}
	logger.Info("local cache directories ready",
		zap.String("query_dir", strings.TrimSpace(cfg.LocalQueryDir)),
		zap.String("log_dir", strings.TrimSpace(cfg.LocalLogDir)),
	)
	return service
}

func (s *Service) Get(ctx context.Context, kind, key string, out any) (bool, error) {
	if kind == KindQuery {
		localHit, err := s.getLocalQuery(ctx, key, out)
		if err == nil && localHit {
			return true, nil
		}
	}
	entry, err := s.store.GetCacheEntry(ctx, kind, key)
	if err == nil {
		if err := json.Unmarshal(entry.Payload, out); err != nil {
			return false, err
		}
		if kind == KindQuery {
			_ = s.setLocalQuery(key, entry.Payload, s.localQueryTTL(s.cfg.QueryTTL))
		}
		return true, nil
	}
	if errors.Is(err, mongostore.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (s *Service) Set(ctx context.Context, kind, key string, payload any, ttl time.Duration) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	localQueryStored := false
	if kind == KindQuery {
		if err := s.setLocalQuery(key, raw, s.localQueryTTL(ttl)); err != nil {
			return err
		}
		localQueryStored = true
		if len(raw) > maxMongoQueryCacheBytes {
			s.logger.Debug("stored large query cache in local filesystem only",
				zap.String("key", key),
				zap.Int("payload_bytes", len(raw)),
				zap.String("query_dir", strings.TrimSpace(s.cfg.LocalQueryDir)),
			)
			return nil
		}
	}
	requestHash, err := util.HashJSON(payload)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := s.store.UpsertCacheEntry(ctx, model.CacheEntry{
		ID:          kind + ":" + key,
		Kind:        kind,
		CacheKey:    key,
		RequestHash: requestHash,
		Payload:     raw,
		CreatedAt:   now,
		ExpireAt:    now.Add(ttl),
	}); err != nil {
		return err
	}
	if kind == KindQuery && !localQueryStored {
		_ = s.setLocalQuery(key, raw, s.localQueryTTL(ttl))
	}
	return nil
}

func (s *Service) QueryTTL() time.Duration {
	return s.cfg.QueryTTL
}

func (s *Service) ServiceListTTL() time.Duration {
	return s.cfg.ServiceListTTL
}

func (s *Service) TagValuesTTL() time.Duration {
	return s.cfg.TagValuesTTL
}

type localQueryEnvelope struct {
	ExpireAt time.Time       `json:"expire_at"`
	Payload  json.RawMessage `json:"payload"`
}

func (s *Service) localQueryTTL(fallback time.Duration) time.Duration {
	if s.cfg.LocalQueryRetention > 0 {
		return s.cfg.LocalQueryRetention
	}
	return fallback
}

func (s *Service) localQueryPath(key string) string {
	dir := strings.TrimSpace(s.cfg.LocalQueryDir)
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "query-"+key+".json")
}

func (s *Service) getLocalQuery(_ context.Context, key string, out any) (bool, error) {
	path := s.localQueryPath(key)
	if path == "" {
		return false, nil
	}
	s.maybeCleanupLocalQueries()

	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	var envelope localQueryEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		_ = os.Remove(path)
		return false, nil
	}
	if !envelope.ExpireAt.IsZero() && time.Now().UTC().After(envelope.ExpireAt) {
		_ = os.Remove(path)
		return false, nil
	}
	if err := json.Unmarshal(envelope.Payload, out); err != nil {
		_ = os.Remove(path)
		return false, nil
	}
	return true, nil
}

func (s *Service) setLocalQuery(key string, payload []byte, ttl time.Duration) error {
	path := s.localQueryPath(key)
	if path == "" {
		return nil
	}
	s.maybeCleanupLocalQueries()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.Marshal(localQueryEnvelope{
		ExpireAt: time.Now().UTC().Add(ttl),
		Payload:  payload,
	})
	if err != nil {
		return err
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, body, 0o644); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tempPath, path)
}

func (s *Service) maybeCleanupLocalQueries() {
	dir := strings.TrimSpace(s.cfg.LocalQueryDir)
	if dir == "" {
		return
	}

	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()

	if time.Since(s.lastLocalCleanup) < 5*time.Minute {
		return
	}
	s.lastLocalCleanup = time.Now()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	now := time.Now().UTC()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "query-") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			_ = os.Remove(path)
			continue
		}
		var envelope localQueryEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil || (!envelope.ExpireAt.IsZero() && now.After(envelope.ExpireAt)) {
			_ = os.Remove(path)
		}
	}
}

func (s *Service) PurgeDatasourceArtifacts(datasourceID string) error {
	logDir := strings.TrimSpace(s.cfg.LocalLogDir)
	if logDir == "" || strings.TrimSpace(datasourceID) == "" {
		return nil
	}

	var firstErr error
	_ = filepath.WalkDir(logDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if name == sanitizeLogPathSegment(datasourceID) || strings.HasSuffix(name, "__"+sanitizeLogPathSegment(datasourceID)) {
			if removeErr := os.RemoveAll(path); removeErr != nil && firstErr == nil {
				firstErr = removeErr
			}
			return filepath.SkipDir
		}
		return nil
	})
	if firstErr == nil {
		s.logger.Info("purged datasource local log artifacts", zap.String("datasource_id", datasourceID), zap.String("log_dir", logDir))
	}
	return firstErr
}
