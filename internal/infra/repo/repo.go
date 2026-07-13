package repo

import (
	"context"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	"gorm.io/gorm"
)

type PostgressReposiroty struct {
	DB *gorm.DB
}

func NewPostgressRepository(DB *gorm.DB) *PostgressReposiroty {
	return &PostgressReposiroty{
		DB: DB,
	}
}

func (r *PostgressReposiroty) Create(ctx context.Context, job *domain.Job) error {
	return r.DB.WithContext(ctx).Create(job).Error
}
