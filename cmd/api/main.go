package main



func main() {
	app , cleanUp := NewApplication()
	defer cleanUp()
	
	app.Mount()
	app.Run()
}
