package httpHandler

import (
	"encoding/json"
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

func (h *JobHandler) HeathCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok."))
}

func (h *JobHandler) CreateJobHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if err := h.validator.Struct(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	maxRetries := req.MaxRetries
	if maxRetries == nil {
		defaultMaxRetries := 5
		maxRetries = &defaultMaxRetries
	}

	timeoutSeconds := req.TimeoutSeconds
	if timeoutSeconds == nil {
		defaultTimeoutSeconds := 60
		timeoutSeconds = &defaultTimeoutSeconds
	}

	job := &domain.Job{
		Type:           req.Type,
		Priority:       req.Priority,
		MaxRetries:     *maxRetries,
		TimeoutSeconds: *timeoutSeconds,
		ScheduledAt:    req.ScheduledAt,
	}

	if err := h.repo.Create(r.Context(), job); err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(job)
}
