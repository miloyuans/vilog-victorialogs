package model

import "time"

type AuditLog struct {
	ID           string         `json:"id" bson:"_id"`
	ResourceType string         `json:"resource_type" bson:"resource_type"`
	ResourceID   string         `json:"resource_id" bson:"resource_id"`
	Action       string         `json:"action" bson:"action"`
	Actor        string         `json:"actor" bson:"actor"`
	Payload      map[string]any `json:"payload,omitempty" bson:"payload,omitempty"`
	CreatedAt    time.Time      `json:"created_at" bson:"created_at"`
}
