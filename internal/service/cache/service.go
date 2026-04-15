package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

const (
	KindQuery       = "query"
	KindServiceList = "service_list"
	KindTagValues   = "tag_values"
)

type Service struct {
	store *mongostore.Store
	cfg   config.CacheConfig
}

func New(store *mongostore.Store, cfg config.CacheConfig) *Service {
	return &Service{
		store: store,
		cfg:   cfg,
	}
}

func (s *Service) Get(ctx context.Context, kind, key string, out any) (bool, error) {
	entry, err := s.store.GetCacheEntry(ctx, kind, key)
	if err == nil {
		if err := json.Unmarshal(entry.Payload, out); err != nil {
			return false, err
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
	requestHash, err := util.HashJSON(payload)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return s.store.UpsertCacheEntry(ctx, model.CacheEntry{
		ID:          kind + ":" + key,
		Kind:        kind,
		CacheKey:    key,
		RequestHash: requestHash,
		Payload:     raw,
		CreatedAt:   now,
		ExpireAt:    now.Add(ttl),
	})
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
