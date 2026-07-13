package domain

import (
	"time"
)

type Job struct {
	ID      string `json:"id" gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
	Status  string `json:"status" gorm:"default:pending"`
	// // retry / backoff
	// RetryCount int `gorm:"not null;default:0"`
	// MaxRetries int `gorm:"not null;default:5"`
	LastError string     `json:"last_error"`
	LockedAt  *time.Time `json:"locked_at"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DoneAt    *time.Time `json:"done_at"`
}

func (Job) TableName() string {
	return "jobs"
}
