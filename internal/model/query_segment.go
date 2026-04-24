package model

import "time"

type QuerySegment struct {
	ID            string     `json:"id" bson:"_id"`
	JobID         string     `json:"job_id" bson:"job_id"`
	Sequence      int64      `json:"sequence" bson:"sequence"`
	FilePath      string     `json:"file_path" bson:"file_path"`
	RowCount      int64      `json:"row_count" bson:"row_count"`
	SizeBytes     int64      `json:"size_bytes" bson:"size_bytes"`
	TimeMin       *time.Time `json:"time_min,omitempty" bson:"time_min,omitempty"`
	TimeMax       *time.Time `json:"time_max,omitempty" bson:"time_max,omitempty"`
	DatasourceIDs []string   `json:"datasource_ids" bson:"datasource_ids"`
	Completed     bool       `json:"completed" bson:"completed"`
	CreatedAt     time.Time  `json:"created_at" bson:"created_at"`
}

type QueryResultsCursor struct {
	JobID          string `json:"job_id"`
	Datasource     string `json:"datasource,omitempty"`
	SegmentSeq     int64  `json:"segment_seq"`
	OffsetInSeg    int64  `json:"offset_in_seg"`
	FilterRevision int64  `json:"filter_revision"`
	Direction      string `json:"direction"`
}

type JobFilterRequest struct {
	Keyword       string              `json:"keyword"`
	KeywordMode   string              `json:"keyword_mode"`
	Start         string              `json:"start"`
	End           string              `json:"end"`
	DatasourceIDs []string            `json:"datasource_ids"`
	ServiceNames  []string            `json:"service_names"`
	Tags          map[string][]string `json:"tags"`
}

type JobResultsPage struct {
	JobID             string              `json:"job_id"`
	Datasource        string              `json:"datasource,omitempty"`
	Status            QueryJobStatus      `json:"status"`
	Results           []SearchResult      `json:"results"`
	Sources           []QuerySourceStatus `json:"sources,omitempty"`
	NextCursor        string              `json:"next_cursor,omitempty"`
	HasMore           bool                `json:"has_more"`
	Completed         bool                `json:"completed"`
	Partial           bool                `json:"partial"`
	MatchedTotalSoFar int64               `json:"matched_total_so_far"`
}
