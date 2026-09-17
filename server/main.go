package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"uuid"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var globalBuffer = NewRingBuffer(1000)

func handleStream(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Upgrade failed:", err)
		return
	}
	defer conn.Close()

	fmt.Println("CLI Connected via WebSocket!")

	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("CLI Disconnected!")
			break
		}

		if msgType == websocket.TextMessage || msgType == websocket.BinaryMessage {
			globalBuffer.Write(msg)

			fmt.Print("SERVER RECEIVED: " + string(msg))
		}
	}
}
func handleSubscribe(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Browser upgrade failed:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Web Browser Viewer Connected!")

	// 1. First, instantly send the viewer all the PAST logs from the Ferris Wheel!
	pastLogs := globalBuffer.GetAll()
	for _, line := range pastLogs {
		conn.WriteMessage(websocket.TextMessage, line)
	}

	// 2. Keep the connection open forever so we can send future logs.
	// (For now, we just loop forever to keep it alive. We will do real Pub/Sub in Milestone 6)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("Web Browser Viewer Disconnected!")
			break
		}
	}
}
func startHttpServer(wg *sync.WaitGroup) *http.Server {
	srv := &http.Server{Addr: ":8080"}

	// Existing endpoints
	http.HandleFunc("/create", func(w http.ResponseWriter, r *http.Request) {
		roomID := uuid.New()
		fmt.Fprintf(w, "Room created: %s", roomID)
	})
	http.HandleFunc("/stream", handleStream)

	// NEW ENDPOINTS:
	http.HandleFunc("/subscribe", handleSubscribe)

	// Serve our HTML file!
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	// ... Keep your WaitGroup / ListenAndServe code here exactly as it is!
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()
	return srv
}
func main() {
	log.Printf("main: starting HTTP server")

	httpServerExitDone := &sync.WaitGroup{}

	startHttpServer(httpServerExitDone)
	httpServerExitDone.Wait()

}
