package ws

import (
	"sync"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
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

	c.Conn.SetReadLimit(4096)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))

	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var msg Message

		err := c.Conn.ReadJSON(&msg)

		if err != nil {
			if logger.DebugEnabled() {
				logger.L().Debug("read pump stopped",
					zap.Int64("user_id", c.UserID),
					zap.Int64("room_id", c.RoomID),
					zap.Error(err),
				)
			}
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
				if logger.DebugEnabled() {
					logger.L().Debug("write pump stopped",
						zap.Int64("user_id", c.UserID),
						zap.Int64("room_id", c.RoomID),
						zap.Error(err),
					)
				}
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))

			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				if logger.DebugEnabled() {
					logger.L().Debug("ping failed, closing",
						zap.Int64("user_id", c.UserID),
						zap.Error(err),
					)
				}
				return
			}
		}
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		err := c.Conn.Close()
		if err != nil && logger.DebugEnabled() {
			logger.L().Debug("close websocket fail",
				zap.Int64("user_id", c.UserID),
				zap.Error(err),
			)
		}
	})
}
