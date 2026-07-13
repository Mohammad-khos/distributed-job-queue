package jobhandler

import "net/http"

// Handler executes the supported background job kinds.
type Handler struct {
	HTTPClient *http.Client
}

// NewHandler creates a handler with a sane default HTTP client.
func NewHandler(client *http.Client) *Handler {
	if client == nil {
		client = &http.Client{}
	}

	return &Handler{HTTPClient: client}
}
