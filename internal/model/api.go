package model

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Error APIError `json:"error"`
}

type StatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
