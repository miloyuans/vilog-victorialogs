package model

type ComponentStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type HealthResponse struct {
	Status     string            `json:"status"`
	Time       string            `json:"time"`
	Version    string            `json:"version,omitempty"`
	Components []ComponentStatus `json:"components,omitempty"`
}
