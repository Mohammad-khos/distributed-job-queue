package main

import "net/http"

func (app *Application) Mount() {
	r := app.Router
	r.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok."))
	})
}
