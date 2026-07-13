package main

import (
	"log"
	"net/http"
)

type Application struct {
	Logger *log.Logger
	Router *http.ServeMux
}

func NewApplication() (*Application, func()) {
	logger := log.Default()
	mux := http.NewServeMux()
	app := &Application{
		Logger: logger,
		Router: mux,
	}

	cleanUp := func() {

	}
	return app, cleanUp
}
