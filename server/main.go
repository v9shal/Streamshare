package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"uuid"
)

func startHttpServer(wg *sync.WaitGroup) *http.Server {
	srv := &http.Server{Addr: ":8080"}

	http.HandleFunc("/create", func(w http.ResponseWriter, r *http.Request) {
		roomID := uuid.New()
		fmt.Fprintf(w, "Room create %s", roomID)

	})
	wg.Add(1)
	go func() {
		defer wg.Done() // let main know we are done cleaning up

		// always returns error. ErrServerClosed on graceful close
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			// unexpected error. port in use?
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	// returning reference so caller can call Shutdown()
	return srv
}
func main() {
	log.Printf("main: starting HTTP server")

	httpServerExitDone := &sync.WaitGroup{}

	startHttpServer(httpServerExitDone)
	httpServerExitDone.Wait()

}
