package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"uuid"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Room struct {
	buffer    *RingBuffer
	client    map[*websocket.Conn]bool
	clientsMu sync.Mutex
}

func NewRoom(clientMap map[*websocket.Conn]bool) *Room {
	return &Room{
		buffer: NewRingBuffer(1000),
		client: clientMap,
	}
}

var globalRoom = make(map[string]*Room)
var RoomMu sync.Mutex

func handleStream(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		fmt.Println("Upgrade failed:", err)
		return
	}
	defer conn.Close()

	fmt.Println("CLI Connected via WebSocket!")

	roomID := r.URL.Query().Get("room")
	RoomMu.Lock()
	myRoom, exists := globalRoom[roomID]
	RoomMu.Unlock()
	if !exists {
		fmt.Errorf("%w", err)
		return
	}

	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("CLI Disconnected!")
			break
		}

		if msgType == websocket.TextMessage || msgType == websocket.BinaryMessage {
			myRoom.buffer.Write(msg)
			myRoom.clientsMu.Lock()
			for viewerConn := range myRoom.client {
				viewerConn.WriteMessage(websocket.TextMessage, msg)
			}
			myRoom.clientsMu.Unlock()
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

	roomID := r.URL.Query().Get("room")
	RoomMu.Lock()
	myRoom, exists := globalRoom[roomID]
	defer func() {
		myRoom.clientsMu.Lock()
		delete(myRoom.client, conn)
		myRoom.clientsMu.Unlock()
	}()
	RoomMu.Unlock()
	if !exists {
		fmt.Errorf("%w", err)
		return
	}
	// 1. First, instantly send the viewer all the PAST logs from the Ferris Wheel!
	pastLogs := myRoom.buffer.GetAll()
	myRoom.clientsMu.Lock()
	myRoom.client[conn] = true
	myRoom.clientsMu.Unlock()
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
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port}

	// Existing endpoints
	http.HandleFunc("/create", func(w http.ResponseWriter, r *http.Request) {
		RoomMu.Lock()

		roomID := uuid.New().String()
		RoomMap := make(map[*websocket.Conn]bool)
		Room := NewRoom(RoomMap)
		fmt.Fprintf(w, "%s", roomID)
		globalRoom[roomID] = Room
		RoomMu.Unlock()
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
