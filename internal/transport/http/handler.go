package httpHandler

import (
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/mohammad-khos/distributed-job-queue/internal/domain"
)

type JobHandler struct {
	repo      domain.Repository
	validator *validator.Validate
}

func NewJobHandler(repo domain.Repository, validator *validator.Validate) *JobHandler {
	return &JobHandler{
		repo:      repo,
		validator: validator,
	}
}
func (h *JobHandler) SaveJob(w http.ResponseWriter, r *http.Request) {
	//validate and save job to database
}
