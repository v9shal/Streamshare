package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	bufferSize      = 1000
	roomIdleTTL     = 30 * time.Minute
	roomAfterEndTTL = 2 * time.Minute
	pingInterval    = 30 * time.Second
	pongWait        = 60 * time.Second
	writeWait       = 10 * time.Second
	maxMessageSize  = 1 << 20
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type client struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *client) writeMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(messageType, data)
}

func (c *client) writeControl(messageType int, data []byte, deadline time.Time) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(messageType, data, deadline)
}

type Room struct {
	id      string
	buffer  *RingBuffer
	created time.Time

	mu           sync.Mutex
	viewers      map[*client]bool
	streamerLive bool
	lastActivity time.Time
}

func NewRoom(id string) *Room {
	now := time.Now()
	return &Room{
		id:           id,
		buffer:       NewRingBuffer(bufferSize),
		created:      now,
		viewers:      make(map[*client]bool),
		lastActivity: now,
	}
}

func (r *Room) addViewer(c *client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.viewers[c] = true
}

func (r *Room) removeViewer(c *client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.viewers, c)
}

func (r *Room) broadcast(messageType int, data []byte) {
	r.mu.Lock()
	viewers := make([]*client, 0, len(r.viewers))
	for v := range r.viewers {
		viewers = append(viewers, v)
	}
	r.mu.Unlock()

	for _, v := range viewers {
		if err := v.writeMessage(messageType, data); err != nil {
			r.removeViewer(v)
			v.conn.Close()
		}
	}
}

func (r *Room) touch() {
	r.mu.Lock()
	r.lastActivity = time.Now()
	r.mu.Unlock()
}

func (r *Room) setStreamerLive(live bool) {
	r.mu.Lock()
	r.streamerLive = live
	r.lastActivity = time.Now()
	r.mu.Unlock()
}

func (r *Room) idleFor() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return time.Since(r.lastActivity)
}

func (r *Room) isStreamerLive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streamerLive
}

type RoomManager struct {
	mu    sync.RWMutex
	rooms map[string]*Room
}

func NewRoomManager() *RoomManager {
	return &RoomManager{rooms: make(map[string]*Room)}
}

func (m *RoomManager) Create() *Room {
	room := NewRoom(uuid.New().String())
	m.mu.Lock()
	m.rooms[room.id] = room
	m.mu.Unlock()
	return room
}

func (m *RoomManager) Get(id string) (*Room, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	room, ok := m.rooms[id]
	return room, ok
}

func (m *RoomManager) delete(id string) {
	m.mu.Lock()
	delete(m.rooms, id)
	m.mu.Unlock()
}
func (m *RoomManager) janitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			var stale []string
			for id, room := range m.rooms {
				idle := room.idleFor()
				if room.isStreamerLive() {
					if idle > roomIdleTTL {
						stale = append(stale, id)
					}
					continue
				}
				if idle > roomAfterEndTTL {
					stale = append(stale, id)
				}
			}
			m.mu.RUnlock()

			for _, id := range stale {
				if room, ok := m.Get(id); ok {
					room.mu.Lock()
					for v := range room.viewers {
						v.conn.Close()
					}
					room.mu.Unlock()
				}
				m.delete(id)
				log.Printf("janitor: reclaimed idle room %s", id)
			}
		}
	}
}

type server struct {
	rooms *RoomManager
}

func (s *server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	room := s.rooms.Create()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(room.id))
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	room, exists := s.rooms.Get(roomID)
	if !exists {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("stream upgrade failed:", err)
		return
	}
	defer conn.Close()

	room.setStreamerLive(true)
	defer room.setStreamerLive(false)

	log.Printf("room %s: streamer connected", roomID)
	defer log.Printf("room %s: streamer disconnected", roomID)

	conn.SetReadLimit(maxMessageSize)
	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		room.touch()
		room.buffer.Write(msg)
		room.broadcast(websocket.TextMessage, msg)
	}
}

func (s *server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	room, exists := s.rooms.Get(roomID)
	if !exists {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("subscribe upgrade failed:", err)
		return
	}
	defer conn.Close()

	c := &client{conn: conn}
	room.addViewer(c)
	defer room.removeViewer(c)
	history := room.buffer.GetAll()

	log.Printf("room %s: viewer connected", roomID)
	defer log.Printf("room %s: viewer disconnected", roomID)

	for _, line := range history {
		if err := c.writeMessage(websocket.TextMessage, line); err != nil {
			return
		}
	}

	runKeepalive(c)
}

func runKeepalive(c *client) {
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := c.conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := c.writeControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				return
			}
		}
	}
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	s := &server{rooms: NewRoomManager()}

	mux := http.NewServeMux()
	mux.HandleFunc("/create", s.handleCreate)
	mux.HandleFunc("/stream", s.handleStream)
	mux.HandleFunc("/subscribe", s.handleSubscribe)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.Handle("/", http.FileServer(http.Dir("./static")))

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go s.rooms.janitor(ctx)

	go func() {
		log.Printf("streamshare-server listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
