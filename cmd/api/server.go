package main

import (
	"context"
	"log"
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
		log.Println("HTTP server started")
		errChan <- srv.ListenAndServe()
	}()

	select {
	case <-cxt.Done():
		log.Println("shutting down due signal")
	case err := <-errChan:
		log.Println("server shtting down due error")
		panic(err)
	}
	// graceful shutdown context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("starting graceful shutdown")

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("could not shutdow server gracefully", "error", err)
		_ = srv.Close()
		panic(err)
	}

}
