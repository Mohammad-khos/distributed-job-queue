package httpHandler

import "time"

type CreateJobRequest struct {
	Type           string     `json:"type" validate:"required"`
	Priority       int        `json:"priority" validate:"min=0,max=100"`
	MaxRetries     *int       `json:"max_retries" validate:"omitempty,min=0,max=20"`
	TimeoutSeconds *int       `json:"timeout_seconds" validate:"omitempty,min=1,max=3600"`
	ScheduledAt    *time.Time `json:"scheduled_at"`
}
