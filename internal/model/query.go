package model

type SearchRequest struct {
	Keyword       string              `json:"keyword"`
	Start         string              `json:"start"`
	End           string              `json:"end"`
	DatasourceIDs []string            `json:"datasource_ids"`
	ServiceNames  []string            `json:"service_names"`
	Tags          map[string][]string `json:"tags"`
	Page          int                 `json:"page"`
	PageSize      int                 `json:"page_size"`
	UseCache      bool                `json:"use_cache"`
}

type SearchResult struct {
	Timestamp  string            `json:"timestamp"`
	Message    string            `json:"message"`
	Service    string            `json:"service"`
	Pod        string            `json:"pod"`
	Datasource string            `json:"datasource"`
	Labels     map[string]string `json:"labels"`
	Raw        map[string]any    `json:"raw"`
}

type QuerySourceStatus struct {
	Datasource string `json:"datasource"`
	Status     string `json:"status"`
	Hits       int    `json:"hits"`
	Error      string `json:"error,omitempty"`
}

type SearchResponse struct {
	Results  []SearchResult      `json:"results"`
	Sources  []QuerySourceStatus `json:"sources"`
	Partial  bool                `json:"partial"`
	CacheHit bool                `json:"cache_hit"`
	TookMS   int64               `json:"took_ms"`
}

type ServiceListResponse struct {
	Services []string `json:"services"`
	CacheHit bool     `json:"cache_hit"`
}

type TagCatalogResponse struct {
	Tags []TagDefinition `json:"tags"`
}

type TagValuesResponse struct {
	Field    string   `json:"field"`
	Values   []string `json:"values"`
	CacheHit bool     `json:"cache_hit"`
}
