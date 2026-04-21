package victorialogs

import "time"

type QueryRequest struct {
	Query  string
	Start  time.Time
	End    time.Time
	Limit  int
	Offset int
}

type QueryChunkRequest struct {
	Query  string
	Start  time.Time
	End    time.Time
	Limit  int
	Offset int
}

type RowHandler func(row map[string]any) error

type ListRequest struct {
	Query       string
	Start       time.Time
	End         time.Time
	Field       string
	Limit       int
	IgnorePipes bool
}

type FieldValuesRequest struct {
	Query       string
	Field       string
	Start       time.Time
	End         time.Time
	Limit       int
	IgnorePipes bool
}

type ValueStat struct {
	Value string `json:"value"`
	Hits  int64  `json:"hits"`
}

type ValuesResponse struct {
	Values []ValueStat `json:"values"`
}

type FacetValue struct {
	FieldValue string `json:"field_value"`
	Hits       int64  `json:"hits"`
}

type Facet struct {
	FieldName string       `json:"field_name"`
	Values    []FacetValue `json:"values"`
}

type FacetsResponse struct {
	Facets []Facet `json:"facets"`
}

type DeleteTaskResponse struct {
	TaskID string `json:"task_id"`
}

type ActiveDeleteTask struct {
	TaskID    string `json:"task_id"`
	Filter    string `json:"filter"`
	StartTime string `json:"start_time"`
}
