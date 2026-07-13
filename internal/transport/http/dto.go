package httpHandler

import "encoding/json"

type CreateJobRequest struct {
	Type    string          `json:"type" validate:"required"`
	Payload json.RawMessage `json:"payload" validate:"required"`
}
