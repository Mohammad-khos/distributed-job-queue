package httpHandler

import "net/http"

type Handler interface {
	SaveJob(w http.ResponseWriter, r *http.Request)
}

type JobHandler struct {
	//validator
}

func (h *JobHandler) SaveJob(w http.ResponseWriter, r *http.Request) {
	//validate and save job to database
}