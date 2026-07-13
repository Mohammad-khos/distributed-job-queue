package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func (app *Application) Run() {
	cxt, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer done()

	srv := http.Server{
		Addr:           ":8888",
		Handler:        app.Router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}
	errChan := make(chan error)

	go func() {
		app.Logger.Infow("HTTP server started", "addr", srv.Addr)
		errChan <- srv.ListenAndServe()
	}()

	select {
	case <-cxt.Done():
		app.Logger.Info("shutting down due signal")
	case err := <-errChan:
		app.Logger.Errorw("server shutting down due error", "error", err)
		panic(err)
	}
	// graceful shutdown context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app.Logger.Info("starting graceful shutdown")

	if err := srv.Shutdown(ctx); err != nil {
		app.Logger.Errorw("could not shut down server gracefully", "error", err)
		_ = srv.Close()
		panic(err)
	}

}
