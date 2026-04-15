package model

import "time"

type TagDefinition struct {
	ID             string    `json:"id" bson:"_id"`
	Name           string    `json:"name" bson:"name"`
	DisplayName    string    `json:"display_name" bson:"display_name"`
	FieldName      string    `json:"field_name" bson:"field_name"`
	UIType         string    `json:"ui_type" bson:"ui_type"`
	Multi          bool      `json:"multi" bson:"multi"`
	Enabled        bool      `json:"enabled" bson:"enabled"`
	DatasourceIDs  []string  `json:"datasource_ids" bson:"datasource_ids"`
	ServiceNames   []string  `json:"service_names" bson:"service_names"`
	AutoDiscovered bool      `json:"auto_discovered" bson:"auto_discovered"`
	Priority       int       `json:"priority" bson:"priority"`
	CreatedAt      time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt      time.Time `json:"updated_at" bson:"updated_at"`
}

type TagDefinitionUpsertRequest struct {
	Name           string   `json:"name"`
	DisplayName    string   `json:"display_name"`
	FieldName      string   `json:"field_name"`
	UIType         string   `json:"ui_type"`
	Multi          *bool    `json:"multi,omitempty"`
	Enabled        *bool    `json:"enabled,omitempty"`
	DatasourceIDs  []string `json:"datasource_ids"`
	ServiceNames   []string `json:"service_names"`
	AutoDiscovered *bool    `json:"auto_discovered,omitempty"`
	Priority       int      `json:"priority,omitempty"`
}

type ServiceCatalogEntry struct {
	ID           string    `json:"id" bson:"_id"`
	DatasourceID string    `json:"datasource_id" bson:"datasource_id"`
	ServiceName  string    `json:"service_name" bson:"service_name"`
	ServiceField string    `json:"service_field" bson:"service_field"`
	LastSeenAt   time.Time `json:"last_seen_at" bson:"last_seen_at"`
	ExpireAt     time.Time `json:"expire_at" bson:"expire_at"`
}

type DatasourceTagSnapshot struct {
	ID                    string    `json:"id" bson:"_id"`
	DatasourceID          string    `json:"datasource_id" bson:"datasource_id"`
	DiscoveredAt          time.Time `json:"discovered_at" bson:"discovered_at"`
	ServiceField          string    `json:"service_field" bson:"service_field"`
	PodField              string    `json:"pod_field" bson:"pod_field"`
	MessageField          string    `json:"message_field" bson:"message_field"`
	TimeField             string    `json:"time_field" bson:"time_field"`
	TagCandidates         []string  `json:"tag_candidates" bson:"tag_candidates"`
	HighCardinalityFields []string  `json:"high_cardinality_fields" bson:"high_cardinality_fields"`
	NotifyStatus          string    `json:"notify_status" bson:"notify_status"`
}

type DiscoveryResponse struct {
	Snapshot DatasourceTagSnapshot `json:"snapshot"`
}
