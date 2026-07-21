package ws

import (
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait = 3 * time.Second

	//PongWait represents the Server send msg to client max waiting time
	pongWait   = 10 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

// Client represents a user connected to a live room through websocket.
type Client struct {
	RoomID   int64
	UserID   int64
	Username string
	Conn     *websocket.Conn

	// send is a buffer channel temporarily outgoing messages.
	Send      chan Message
	closeOnce sync.Once
}

func NewClient(roomID int64, userId int64, username string, conn *websocket.Conn) *Client {
	return &Client{
		RoomID:   roomID,
		UserID:   userId,
		Conn:     conn,
		Username: username,
		Send:     make(chan Message, 256),
	}
}

// ReadPump read the message from the frontend.
// dispatch msg through ActionRegistry
func (c *Client) ReadPump(registry *ActionRegistry) {

	c.Conn.SetReadDeadline(time.Now().Add(pongWait))

	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var msg Message

		err := c.Conn.ReadJSON(&msg)

		if err != nil {
			log.Printf("read pump stopped (user=%d, room=%d): %v", c.UserID, c.RoomID, err)
			return
		}

		c.Conn.SetReadDeadline(time.Now().Add(pongWait))

		registry.Dispatch(c, msg)

	}
}

// WritePump write the message from backend to the frontend
func (c *Client) WritePump() {

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {

		case msg, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))

			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteJSON(msg); err != nil {
				log.Printf("write pump stopped (fall) : %v", err)
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))

			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("write pump stopped (ping fail, user=%d): %v", c.UserID, err)
				return
			}

		}

	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		err := c.Conn.Close()
		if err != nil {
			log.Printf("fail to close websocket connection: %v", err)
		}
	})
}
