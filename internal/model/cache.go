package model

import "time"

type CacheEntry struct {
	ID          string    `json:"id" bson:"_id"`
	Kind        string    `json:"kind" bson:"kind"`
	CacheKey    string    `json:"cache_key" bson:"cache_key"`
	RequestHash string    `json:"request_hash" bson:"request_hash"`
	Payload     []byte    `json:"-" bson:"payload"`
	CreatedAt   time.Time `json:"created_at" bson:"created_at"`
	ExpireAt    time.Time `json:"expire_at" bson:"expire_at"`
}
