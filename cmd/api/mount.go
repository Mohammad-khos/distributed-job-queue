package main

import (
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

func (app *Application) Mount() {
	r := app.Router

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/health", app.Handler.HeathCheckHandler)
	r.Post("/jobs", app.Handler.CreateJobHandler)
}
