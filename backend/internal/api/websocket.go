package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zde37/torus/pkg"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 512

	// Size of the send buffer per client
	sendBufferSize = 256
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // TODO: Configure in production
	},
}

// client represents a connected WebSocket client.
type client struct {
	hub  *WebSocketHub
	conn *websocket.Conn
	send chan []byte // Buffered channel of outbound messages
}

// WebSocketHub manages WebSocket connections for live updates.
type WebSocketHub struct {
	// Registered clients
	clients map[*client]bool

	// Inbound messages from clients (currently unused, for future)
	broadcast chan []byte

	// Register requests from clients
	register chan *client

	// Unregister requests from clients
	unregister chan *client

	// Shutdown signal
	shutdown chan struct{}

	// WaitGroup for graceful shutdown
	wg sync.WaitGroup

	// Mutex for thread-safe access to clients map
	mu sync.RWMutex

	// Logger
	logger *pkg.Logger
}

// NewWebSocketHub creates a new WebSocket hub.
func NewWebSocketHub(logger *pkg.Logger) *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[*client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *client),
		unregister: make(chan *client),
		shutdown:   make(chan struct{}),
		logger:     logger,
	}
}

// Run starts the WebSocket hub.
func (h *WebSocketHub) Run() {
	h.wg.Add(1)
	defer h.wg.Done()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.logger.Info("client connected", pkg.Fields{
				"total_clients": len(h.clients),
			})

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				h.mu.Unlock()
				h.logger.Info("client disconnected", pkg.Fields{
					"total_clients": len(h.clients),
				})
			} else {
				h.mu.Unlock()
			}

		case message := <-h.broadcast:
			h.mu.RLock()
			for c := range h.clients {
				select {
				case c.send <- message:
					// Message sent successfully
				default:
					// Client's send buffer is full (slow client)
					// Close and unregister the client
					h.mu.RUnlock()
					h.logger.Warn("client send buffer full, disconnecting slow client", nil)
					go func(cl *client) {
						h.unregister <- cl
					}(c)
					h.mu.RLock()
				}
			}
			h.mu.RUnlock()

		case <-h.shutdown:
			h.logger.Info("shutting down WebSocket hub", nil)
			// Close all client connections
			h.mu.Lock()
			for c := range h.clients {
				close(c.send)
				c.conn.Close()
				delete(h.clients, c)
			}
			h.mu.Unlock()
			h.logger.Info("webSocket hub shutdown complete", nil)
			return
		}
	}
}

// Stop gracefully shuts down the WebSocket hub.
func (h *WebSocketHub) Stop() {
	close(h.shutdown)
	h.wg.Wait()
}

// readPump pumps messages from the WebSocket connection to the hub.
// The application runs readPump in a per-connection goroutine.
func (c *client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Read messages from client (currently just for keep-alive)
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.hub.logger.Error("webSocket unexpected close error", pkg.Fields{"error": err.Error()})
			}
			break
		}
		// TODO: Handle incoming messages here
	}
}

// writePump pumps messages from the hub to the WebSocket connection.
// A goroutine running writePump is started for each connection.
// The application ensures that there is at most one writer to a connection
// by executing all writes from this goroutine.
func (c *client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current WebSocket message
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// HandleWebSocket handles WebSocket connections.
func (h *WebSocketHub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("failed to upgrade to websocket", pkg.Fields{"error": err.Error()})
		return
	}

	client := &client{
		hub:  h,
		conn: conn,
		send: make(chan []byte, sendBufferSize),
	}

	h.register <- client

	// Start client goroutines
	// Each client has exactly ONE writer goroutine (writePump)
	// and ONE reader goroutine (readPump)
	go client.writePump()
	go client.readPump()
}

// BroadcastRingUpdate sends a ring update to all connected clients.
func (h *WebSocketHub) BroadcastRingUpdate(update interface{}) error {
	data, err := json.Marshal(update)
	if err != nil {
		return err
	}

	select {
	case h.broadcast <- data:
		// Message queued successfully
	default:
		h.logger.Warn("broadcast channel full, dropping message", nil)
	}

	return nil
}
