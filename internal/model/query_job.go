package model

import "time"

type QueryJobStatus string

const (
	QueryJobPending   QueryJobStatus = "pending"
	QueryJobRunning   QueryJobStatus = "running"
	QueryJobCompleted QueryJobStatus = "completed"
	QueryJobFailed    QueryJobStatus = "failed"
	QueryJobCancelled QueryJobStatus = "cancelled"
	QueryJobPartial   QueryJobStatus = "partial"
)

type QueryJob struct {
	ID             string             `json:"id" bson:"_id"`
	Request        SearchRequest      `json:"request" bson:"request"`
	Status         QueryJobStatus     `json:"status" bson:"status"`
	StartedAt      time.Time          `json:"started_at" bson:"started_at"`
	FinishedAt     *time.Time         `json:"finished_at,omitempty" bson:"finished_at,omitempty"`
	ExpiresAt      time.Time          `json:"expires_at" bson:"expires_at"`
	Progress       QueryJobProgress   `json:"progress" bson:"progress"`
	Totals         QueryJobTotals     `json:"totals" bson:"totals"`
	SourceStates   []QuerySourceState `json:"source_states" bson:"source_states"`
	LastError      string             `json:"last_error,omitempty" bson:"last_error,omitempty"`
	FilterRevision int64              `json:"filter_revision" bson:"filter_revision"`
	ResultVersion  int64              `json:"result_version" bson:"result_version"`
}

type QueryJobProgress struct {
	DatasourceTotal     int   `json:"datasource_total" bson:"datasource_total"`
	DatasourceRunning   int   `json:"datasource_running" bson:"datasource_running"`
	DatasourceCompleted int   `json:"datasource_completed" bson:"datasource_completed"`
	DatasourceFailed    int   `json:"datasource_failed" bson:"datasource_failed"`
	SegmentsWritten     int64 `json:"segments_written" bson:"segments_written"`
	RowsWritten         int64 `json:"rows_written" bson:"rows_written"`
	RowsMatched         int64 `json:"rows_matched" bson:"rows_matched"`
	BytesWritten        int64 `json:"bytes_written" bson:"bytes_written"`
}

type QueryJobTotals struct {
	RowsTotal   int64 `json:"rows_total" bson:"rows_total"`
	RowsMatched int64 `json:"rows_matched" bson:"rows_matched"`
}

type QuerySourceState struct {
	DatasourceID    string     `json:"datasource_id" bson:"datasource_id"`
	DatasourceName  string     `json:"datasource_name" bson:"datasource_name"`
	Status          string     `json:"status" bson:"status"`
	StartedAt       *time.Time `json:"started_at,omitempty" bson:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty" bson:"finished_at,omitempty"`
	RowsFetched     int64      `json:"rows_fetched" bson:"rows_fetched"`
	RowsMatched     int64      `json:"rows_matched" bson:"rows_matched"`
	SegmentsWritten int64      `json:"segments_written" bson:"segments_written"`
	Partial         bool       `json:"partial" bson:"partial"`
	Error           string     `json:"error,omitempty" bson:"error,omitempty"`
}

type QueryJobCreateResponse struct {
	JobID  string         `json:"job_id"`
	Status QueryJobStatus `json:"status"`
}

type QueryJobEvent struct {
	Type         string              `json:"type"`
	JobID        string              `json:"job_id"`
	Status       QueryJobStatus      `json:"status,omitempty"`
	Sequence     int64               `json:"sequence,omitempty"`
	FromSequence int64               `json:"from_sequence,omitempty"`
	ToSequence   int64               `json:"to_sequence,omitempty"`
	RowCount     int64               `json:"row_count,omitempty"`
	DatasourceID string              `json:"datasource_id,omitempty"`
	Datasource   string              `json:"datasource,omitempty"`
	Progress     QueryJobProgress    `json:"progress,omitempty"`
	Sources      []QuerySourceStatus `json:"sources,omitempty"`
	Rows         []SearchResult      `json:"rows,omitempty"`
	LastError    string              `json:"error,omitempty"`
}
