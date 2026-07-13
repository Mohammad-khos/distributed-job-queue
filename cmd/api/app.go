package main

import (
	"github.com/go-chi/chi/v5"
	"github.com/mohammad-khos/distributed-job-queue/internal/infra/repo"
	httpHandler "github.com/mohammad-khos/distributed-job-queue/internal/transport/http"
	"github.com/mohammad-khos/distributed-job-queue/pkg/db"
	"github.com/mohammad-khos/distributed-job-queue/pkg/env"
	"github.com/mohammad-khos/distributed-job-queue/pkg/validation"
	"go.uber.org/zap"
)

type Application struct {
	Handler *httpHandler.JobHandler
	Logger  *zap.SugaredLogger
	Router  chi.Router
}

func NewApplication() (*Application, func()) {
	logger := zap.Must(zap.NewProduction()).Sugar()
	router := chi.NewRouter()
	validator := validation.NewValidator()
	DB, err := db.New(
		env.GetString("DB_URI", ""),
		env.GetInt("DB_MAX_OPEN_CONNS", 30),
		env.GetInt("DB_MAX_IDLE_CONNS", 30),
		env.GetString("DB_MAX_IDLE_TIME", "15m"),
	)
	sqlDB, _ := DB.DB()
	if err != nil {
		logger.Fatalw("failed to connect database", "error", err)
	}
	logger.Infow("database connection established")
	repo := repo.NewPostgressRepository(DB)
	handler := httpHandler.NewJobHandler(repo, validator)

	cleanUp := func() {
		_ = logger.Sync()
		if err := sqlDB.Close(); err != nil {
			logger.Errorw("failed to close database connection", "error", err)
		}
	}
	app := &Application{
		Handler: handler,
		Logger:  logger,
		Router:  router,
	}
	return app, cleanUp
}
