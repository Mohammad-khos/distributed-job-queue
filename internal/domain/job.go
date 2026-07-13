package domain

import "time"

type Job struct {
	ID             string     `json:"id" gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	Type           string     `json:"type"`
	Priority       int        `json:"priority" gorm:"default:0"`
	Status         string     `json:"status" gorm:"default:pending"`
	RetryCount     int        `json:"retry_count" gorm:"not null;default:0"`
	MaxRetries     int        `json:"max_retries" gorm:"not null;default:5"`
	TimeoutSeconds int        `json:"timeout_seconds" gorm:"not null;default:60"`
	LastError      string     `json:"last_error"`
	LockedAt       *time.Time `json:"locked_at"`
	ScheduledAt    *time.Time `json:"scheduled_at"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DoneAt         *time.Time `json:"done_at"`
}

func (Job) TableName() string {
	return "jobs"
}
