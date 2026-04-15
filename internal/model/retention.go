package model

import "time"

type RetentionPolicyTemplate struct {
	ID            string    `json:"id" bson:"_id"`
	Name          string    `json:"name" bson:"name"`
	RetentionDays int       `json:"retention_days" bson:"retention_days"`
	Cron          string    `json:"cron" bson:"cron"`
	Enabled       bool      `json:"enabled" bson:"enabled"`
	CreatedAt     time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt     time.Time `json:"updated_at" bson:"updated_at"`
}

type RetentionPolicyTemplateUpsertRequest struct {
	Name          string `json:"name"`
	RetentionDays int    `json:"retention_days"`
	Cron          string `json:"cron"`
	Enabled       *bool  `json:"enabled,omitempty"`
}

type DatasourceRetentionBinding struct {
	ID               string              `json:"id" bson:"_id"`
	DatasourceID     string              `json:"datasource_id" bson:"datasource_id"`
	PolicyTemplateID string              `json:"policy_template_id" bson:"policy_template_id"`
	Enabled          bool                `json:"enabled" bson:"enabled"`
	ServiceScope     []string            `json:"service_scope" bson:"service_scope"`
	TagScope         map[string][]string `json:"tag_scope" bson:"tag_scope"`
	LastRunAt        *time.Time          `json:"last_run_at,omitempty" bson:"last_run_at,omitempty"`
	LastTaskID       string              `json:"last_task_id,omitempty" bson:"last_task_id,omitempty"`
	LastStatus       string              `json:"last_status,omitempty" bson:"last_status,omitempty"`
	CreatedAt        time.Time           `json:"created_at" bson:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at" bson:"updated_at"`
}

type DatasourceRetentionBindingUpsertRequest struct {
	DatasourceID     string              `json:"datasource_id"`
	PolicyTemplateID string              `json:"policy_template_id"`
	Enabled          *bool               `json:"enabled,omitempty"`
	ServiceScope     []string            `json:"service_scope"`
	TagScope         map[string][]string `json:"tag_scope"`
}

type DeleteTask struct {
	ID           string     `json:"id" bson:"_id"`
	DatasourceID string     `json:"datasource_id" bson:"datasource_id"`
	BindingID    string     `json:"binding_id,omitempty" bson:"binding_id,omitempty"`
	TaskID       string     `json:"task_id,omitempty" bson:"task_id,omitempty"`
	Filter       string     `json:"filter" bson:"filter"`
	Status       string     `json:"status" bson:"status"`
	StartedAt    time.Time  `json:"started_at" bson:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty" bson:"finished_at,omitempty"`
	ErrorMsg     string     `json:"error_msg,omitempty" bson:"error_msg,omitempty"`
}

type RetentionRunResponse struct {
	DatasourceID string       `json:"datasource_id"`
	Tasks        []DeleteTask `json:"tasks"`
}
