package mongostore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"vilog-victorialogs/internal/model"
)

func (s *Store) GetCacheEntry(ctx context.Context, kind, cacheKey string) (model.CacheEntry, error) {
	var entry model.CacheEntry
	err := s.collection(collectionQueryCache).FindOne(ctx, bson.M{
		"kind":      kind,
		"cache_key": cacheKey,
		"expire_at": bson.M{"$gt": time.Now().UTC()},
	}).Decode(&entry)
	if err == nil {
		return entry, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.CacheEntry{}, ErrNotFound
	}
	return model.CacheEntry{}, fmt.Errorf("find cache entry: %w", err)
}

func (s *Store) UpsertCacheEntry(ctx context.Context, entry model.CacheEntry) error {
	_, err := s.collection(collectionQueryCache).UpdateOne(
		ctx,
		bson.M{"kind": entry.Kind, "cache_key": entry.CacheKey},
		bson.M{
			"$set": bson.M{
				"kind":         entry.Kind,
				"cache_key":    entry.CacheKey,
				"request_hash": entry.RequestHash,
				"payload":      entry.Payload,
				"created_at":   entry.CreatedAt,
				"expire_at":    entry.ExpireAt,
			},
			"$setOnInsert": bson.M{
				"_id": entry.ID,
			},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upsert cache entry: %w", err)
	}
	return nil
}

func (s *Store) DeleteCacheEntriesByPrefix(ctx context.Context, kind, prefix string) error {
	filter := bson.M{
		"kind": kind,
		"cache_key": bson.M{
			"$regex": primitive.Regex{
				Pattern: "^" + regexp.QuoteMeta(prefix),
				Options: "",
			},
		},
	}
	if _, err := s.collection(collectionQueryCache).DeleteMany(ctx, filter); err != nil {
		return fmt.Errorf("delete cache entries by prefix: %w", err)
	}
	return nil
}
