package ws

import (
	"log"

	"github.com/gorilla/websocket"
)

// Client represents a user connected to a live room through websocket.
type Client struct {
	RoomID   int64
	UserID   int64
	Username string
	Conn     *websocket.Conn

	// send is a buffer channel temporarily outgoing messages.
	Send chan Message
}

func NewClient(roomID int64, userId int64, username string, conn *websocket.Conn) *Client {
	return &Client{
		RoomID:   roomID,
		UserID:   userId,
		Conn:     conn,
		Username: username,

		Send: make(chan Message, 256),
	}
}

// ReadPump read the message from the frontend.
func (c *Client) ReadPump(manager *Manager) {

	for {
		var msg Message

		err := c.Conn.ReadJSON(&msg)

		if err != nil {
			log.Printf("fail to read message through websocket : %v", err)
			return
		}

		// 当前连接上保存的信息覆盖。
		outmsg := NewChatMessage(c.UserID, c.RoomID, c.Username, msg.Content)
		manager.BroadcastToRoom(c.RoomID, outmsg)

	}
}

// WritePump write the message from backend to the frontend
func (c *Client) WritePump() {
	
	for msg := range c.Send {
		err := c.Conn.WriteJSON(msg)
		if err != nil {
			log.Printf("fail to write mesasge through websocket : %v", err)
			return
		}
	}

}

func (c *Client) Close() {
	err := c.Conn.Close()
	if err != nil {
		log.Printf("fail to close websocket connection: %v", err)
	}
}
