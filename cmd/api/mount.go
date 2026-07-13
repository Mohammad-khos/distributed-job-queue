package main

func (app *Application) Mount() {
	r := app.Router

	r.HandleFunc("GET /health" , app.Handler.HeathCheckHandler)
	r.HandleFunc("POST /jobs", app.Handler.CreateJobHandler)
}
