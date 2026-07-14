package repo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PostgresRepository struct {
	DB *gorm.DB
}

func NewPostgresRepository(db *gorm.DB) *PostgresRepository {
	return &PostgresRepository{
		DB: db,
	}
}

func NewPostgressRepository(db *gorm.DB) *PostgresRepository {
	return NewPostgresRepository(db)
}

func (r *PostgresRepository) Create(
	ctx context.Context,
	job *domain.Job,
) error {
	db, err := r.database(ctx)
	if err != nil {
		return err
	}

	if job == nil {
		return errors.New("repository: job is nil")
	}

	if strings.TrimSpace(job.Type) == "" {
		return errors.New("repository: job type is required")
	}

	if strings.TrimSpace(job.Status) == "" {
		job.Status = domain.JobStatusPending
	}

	if err := db.Create(job).Error; err != nil {
		return fmt.Errorf("repository: create job: %w", err)
	}

	return nil
}

func (r *PostgresRepository) GetByID(
	ctx context.Context,
	jobID string,
) (*domain.Job, error) {
	db, err := r.database(ctx)
	if err != nil {
		return nil, err
	}

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, errors.New("repository: job id is required")
	}

	var job domain.Job

	err = db.
		Where("id = ?", jobID).
		Take(&job).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrJobNotFound
	}

	if err != nil {
		return nil, fmt.Errorf(
			"repository: get job %q: %w",
			jobID,
			err,
		)
	}

	return &job, nil
}

func (r *PostgresRepository) ClaimQueuedJobs(
	ctx context.Context,
	limit uint,
) ([]*domain.Job, error) {
	db, err := r.database(ctx)
	if err != nil {
		return nil, err
	}

	if limit == 0 {
		return []*domain.Job{}, nil
	}

	now := time.Now().UTC()

	claimedJobs := make([]*domain.Job, 0, limit)

	err = db.Transaction(func(tx *gorm.DB) error {
		var jobs []domain.Job

		query := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
				Options:  "SKIP LOCKED",
			}).
			Where("status = ?", domain.JobStatusPending).
			Where(
				"(scheduled_at IS NULL OR scheduled_at <= ?)",
				now,
			).
			Order(
				"priority DESC, scheduled_at ASC NULLS FIRST, created_at ASC",
			).
			Limit(int(limit))

		if err := query.Find(&jobs).Error; err != nil {
			return fmt.Errorf(
				"repository: select queued jobs: %w",
				err,
			)
		}

		if len(jobs) == 0 {
			claimedJobs = []*domain.Job{}
			return nil
		}

		jobIDs := make([]string, 0, len(jobs))
		for i := range jobs {
			jobIDs = append(jobIDs, jobs[i].ID)
		}

		result := tx.
			Model(&domain.Job{}).
			Where("id IN ?", jobIDs).
			Where("status = ?", domain.JobStatusPending).
			Updates(map[string]any{
				"status":     domain.JobStatusReserved,
				"locked_at":  now,
				"updated_at": now,
			})

		if result.Error != nil {
			return fmt.Errorf(
				"repository: reserve queued jobs: %w",
				result.Error,
			)
		}

		if result.RowsAffected != int64(len(jobs)) {
			return fmt.Errorf(
				"repository: expected to reserve %d jobs, reserved %d",
				len(jobs),
				result.RowsAffected,
			)
		}

		claimedJobs = make([]*domain.Job, 0, len(jobs))

		for i := range jobs {
			jobs[i].Status = domain.JobStatusReserved
			jobs[i].LockedAt = timePointer(now)
			jobs[i].UpdatedAt = now

			claimedJobs = append(
				claimedJobs,
				&jobs[i],
			)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf(
			"repository: claim queued jobs: %w",
			err,
		)
	}

	return claimedJobs, nil
}

func (r *PostgresRepository) MarkJobProcessing(
	ctx context.Context,
	jobID string,
) error {
	return r.transitionJob(
		ctx,
		jobID,
		[]string{
			domain.JobStatusReserved,
		},
		domain.JobStatusProcessing,
		func(now time.Time) map[string]any {
			return map[string]any{
				"last_error": "",
				"done_at":    nil,
			}
		},
	)
}

func (r *PostgresRepository) MarkJobDone(
	ctx context.Context,
	jobID string,
) error {
	return r.transitionJob(
		ctx,
		jobID,
		[]string{
			domain.JobStatusProcessing,
		},
		domain.JobStatusDone,
		func(now time.Time) map[string]any {
			return map[string]any{
				"last_error": "",
				"locked_at":  nil,
				"done_at":    now,
			}
		},
	)
}

func (r *PostgresRepository) MarkJobFailed(
	ctx context.Context,
	jobID string,
	lastError string,
) error {
	return r.transitionJob(
		ctx,
		jobID,
		[]string{
			domain.JobStatusReserved,
			domain.JobStatusProcessing,
		},
		domain.JobStatusFailed,
		func(now time.Time) map[string]any {
			return map[string]any{
				"last_error": lastError,
				"locked_at":  nil,
				"done_at":    now,
			}
		},
	)
}

func (r *PostgresRepository) ReleaseReservedJob(
	ctx context.Context,
	jobID string,
) error {
	return r.transitionJob(
		ctx,
		jobID,
		[]string{
			domain.JobStatusReserved,
		},
		domain.JobStatusPending,
		func(now time.Time) map[string]any {
			return map[string]any{
				"locked_at":  nil,
				"done_at":    nil,
				"last_error": "",
			}
		},
	)
}

func (r *PostgresRepository) transitionJob(
	ctx context.Context,
	jobID string,
	allowedStatuses []string,
	targetStatus string,
	buildUpdates func(now time.Time) map[string]any,
) error {
	db, err := r.database(ctx)
	if err != nil {
		return err
	}

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return errors.New("repository: job id is required")
	}

	return db.Transaction(func(tx *gorm.DB) error {
		var job domain.Job

		err := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
			}).
			Where("id = ?", jobID).
			Take(&job).
			Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.ErrJobNotFound
		}

		if err != nil {
			return fmt.Errorf(
				"repository: lock job %q: %w",
				jobID,
				err,
			)
		}

		if job.Status == targetStatus {
			return nil
		}

		if !containsStatus(allowedStatuses, job.Status) {
			return &domain.InvalidJobTransitionError{
				JobID: job.ID,
				From:  job.Status,
				To:    targetStatus,
			}
		}

		now := time.Now().UTC()

		updates := map[string]any{
			"status":     targetStatus,
			"updated_at": now,
		}

		if buildUpdates != nil {
			for column, value := range buildUpdates(now) {
				updates[column] = value
			}
		}

		result := tx.
			Model(&domain.Job{}).
			Where("id = ?", job.ID).
			Where("status = ?", job.Status).
			Updates(updates)

		if result.Error != nil {
			return fmt.Errorf(
				"repository: update job %q from %q to %q: %w",
				job.ID,
				job.Status,
				targetStatus,
				result.Error,
			)
		}

		if result.RowsAffected != 1 {
			return fmt.Errorf(
				"repository: job %q status update affected %d rows",
				job.ID,
				result.RowsAffected,
			)
		}

		return nil
	})
}

func (r *PostgresRepository) database(
	ctx context.Context,
) (*gorm.DB, error) {
	if ctx == nil {
		return nil, errors.New(
			"repository: context is nil",
		)
	}

	if r == nil || r.DB == nil {
		return nil, errors.New(
			"repository: database is not configured",
		)
	}

	return r.DB.WithContext(ctx), nil
}

func containsStatus(
	statuses []string,
	target string,
) bool {
	for _, status := range statuses {
		if status == target {
			return true
		}
	}

	return false
}

func timePointer(value time.Time) *time.Time {
	return &value
}
