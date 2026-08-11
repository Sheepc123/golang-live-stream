package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait = 3 * time.Second

	//PongWait represents the Server send msg to client max waiting time
	pongWait   = 10 * time.Second
	pingPeriod = (pongWait * 9) / 10

	maxMessageSize = 2048
	sendBuffer     = 32
)

// Client represents a user connected to a live room through websocket.
type Client struct {
	RoomID   int64
	UserID   int64
	Username string
	Conn     *websocket.Conn

	// send is a buffer channel saving slice header
	// readOnly
	Send      chan []byte
	closeOnce sync.Once
}

func NewClient(roomID int64, userId int64, username string, conn *websocket.Conn) *Client {
	return &Client{
		RoomID:   roomID,
		UserID:   userId,
		Conn:     conn,
		Username: username,
		Send:     make(chan []byte, sendBuffer),
	}
}

// ReadPump read the message from the frontend.
// dispatch msg through ActionRegistry
func (c *Client) ReadPump(registry *ActionRegistry) {

	c.Conn.SetReadLimit(maxMessageSize)
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

		case payload, ok := <-c.Send:

			if !ok {
				c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))

			if err := c.Conn.WriteMessage(websocket.TextMessage, payload); err != nil {
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

// Send Message only one person
// without Redis and broadcast pool.
func (c *Client) SendMsgOnlyOne(msg Message) bool {
	payload, err := json.Marshal(msg)

	if err != nil {
		logger.L().Error("encode direct message fail",
			zap.String("type", msg.Type),
			zap.Int64("user_id", c.UserID),
			zap.Error(err),
		)
		return false
	}

	select {
	case c.Send <- payload:
		metrics.WSMessages.WithLabelValues("down", msg.Type).Inc()
		return true
	default:
		metrics.WSDropped.WithLabelValues("client_slow").Inc()
		return false
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
