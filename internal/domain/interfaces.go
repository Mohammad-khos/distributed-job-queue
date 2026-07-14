package domain

import "context"

type Repository interface {
	Create(ctx context.Context, job *Job) error

	GetByID(ctx context.Context, jobID string) (*Job, error)

	ClaimQueuedJobs(
		ctx context.Context,
		limit uint,
	) ([]*Job, error)

	MarkJobProcessing(
		ctx context.Context,
		jobID string,
	) error

	MarkJobDone(
		ctx context.Context,
		jobID string,
	) error

	MarkJobFailed(
		ctx context.Context,
		jobID string,
		lastError string,
	) error

	ReleaseReservedJob(
		ctx context.Context,
		jobID string,
	) error
}