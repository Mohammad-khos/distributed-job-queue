package main

import (
	"net/http"

	"go.uber.org/zap"
)

type Application struct {
	Logger *zap.SugaredLogger
	Router *http.ServeMux
}

func NewApplication() (*Application, func()) {
	logger := zap.Must(zap.NewProduction()).Sugar()
	mux := http.NewServeMux()
	app := &Application{
		Logger: logger,
		Router: mux,
	}

	cleanUp := func() {
		_ = logger.Sync()
	}
	return app, cleanUp
}
