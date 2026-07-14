package domain

import "context"

type Repository interface {
	Create(ctx context.Context, job *Job) error
	ClaimQueuedJobs(ctx context.Context , limit uint) ([]*Job , error)
}
