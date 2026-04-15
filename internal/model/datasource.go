package model

import "time"

type Datasource struct {
	ID             string                 `json:"id" bson:"_id"`
	Name           string                 `json:"name" bson:"name"`
	BaseURL        string                 `json:"base_url" bson:"base_url"`
	Enabled        bool                   `json:"enabled" bson:"enabled"`
	TimeoutSeconds int                    `json:"timeout_seconds" bson:"timeout_seconds"`
	Headers        DatasourceHeaders      `json:"headers" bson:"headers"`
	QueryPaths     DatasourceQueryPaths   `json:"query_paths" bson:"query_paths"`
	FieldMapping   DatasourceFieldMapping `json:"field_mapping" bson:"field_mapping"`
	SupportsDelete bool                   `json:"supports_delete" bson:"supports_delete"`
	CreatedAt      time.Time              `json:"created_at" bson:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at" bson:"updated_at"`
}

type DatasourceHeaders struct {
	AccountID     string `json:"AccountID,omitempty" bson:"AccountID,omitempty"`
	ProjectID     string `json:"ProjectID,omitempty" bson:"ProjectID,omitempty"`
	Authorization string `json:"Authorization,omitempty" bson:"Authorization,omitempty"`
}

type DatasourceQueryPaths struct {
	Query             string `json:"query" bson:"query"`
	FieldNames        string `json:"field_names" bson:"field_names"`
	FieldValues       string `json:"field_values" bson:"field_values"`
	StreamFieldNames  string `json:"stream_field_names" bson:"stream_field_names"`
	StreamFieldValues string `json:"stream_field_values" bson:"stream_field_values"`
	Facets            string `json:"facets" bson:"facets"`
	DeleteRunTask     string `json:"delete_run_task" bson:"delete_run_task"`
	DeleteActiveTasks string `json:"delete_active_tasks" bson:"delete_active_tasks"`
	DeleteStopTask    string `json:"delete_stop_task" bson:"delete_stop_task"`
}

type DatasourceFieldMapping struct {
	ServiceField string `json:"service_field" bson:"service_field"`
	PodField     string `json:"pod_field" bson:"pod_field"`
	MessageField string `json:"message_field" bson:"message_field"`
	TimeField    string `json:"time_field" bson:"time_field"`
}

type DatasourceUpsertRequest struct {
	Name           string                 `json:"name"`
	BaseURL        string                 `json:"base_url"`
	Enabled        *bool                  `json:"enabled,omitempty"`
	TimeoutSeconds int                    `json:"timeout_seconds,omitempty"`
	Headers        DatasourceHeaders      `json:"headers"`
	QueryPaths     DatasourceQueryPaths   `json:"query_paths"`
	FieldMapping   DatasourceFieldMapping `json:"field_mapping"`
	SupportsDelete *bool                  `json:"supports_delete,omitempty"`
}

type DatasourceTestResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

func DefaultDatasourceQueryPaths() DatasourceQueryPaths {
	return DatasourceQueryPaths{
		Query:             "/select/logsql/query",
		FieldNames:        "/select/logsql/field_names",
		FieldValues:       "/select/logsql/field_values",
		StreamFieldNames:  "/select/logsql/stream_field_names",
		StreamFieldValues: "/select/logsql/stream_field_values",
		Facets:            "/select/logsql/facets",
		DeleteRunTask:     "/delete/run_task",
		DeleteActiveTasks: "/delete/active_tasks",
		DeleteStopTask:    "/delete/stop_task",
	}
}

func DefaultDatasourceFieldMapping() DatasourceFieldMapping {
	return DatasourceFieldMapping{
		TimeField: "_time",
	}
}
