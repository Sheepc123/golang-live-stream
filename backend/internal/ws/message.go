package ws

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/service"
)

// Message Type includes:
// Join,Leave the live stream
// Chat Content AKA danmu
// heartbeat
// system message

const (
	MessageTypeJoin        = "join"
	MessageTypeLeave       = "leave"
	MessageTypeChat        = "chat"
	MessageTypeHeartBeat   = "heartbeat"
	MessageTypeSystem      = "system"
	MessageTypeError       = "error"
	MessageTypeOnlineCount = "online_count"
	MessageTypeLike        = "like"
	MessageTypeLikeCount   = "like_count"
)

// Message represents the message in websocket.
type Message struct {
	Type      string `json:"type"`
	RoomID    int64  `json:"room_id"`
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

// newMessage is the local-level constructor reponsible for populating
// the public fields such as RoomID,Type
func newMessage(roomId int64, Type string) Message {
	return Message{
		RoomID:    roomId,
		Type:      Type,
		Timestamp: time.Now().Unix(),
	}
}

// NewChatMessage creates a new Chat message
func NewChatMessage(userid, roomid int64, username, Content string) Message {
	msg := newMessage(roomid, MessageTypeChat)
	msg.UserID = userid
	msg.Content = Content
	msg.Username = username
	return msg
}

// NewJoinMessage creates a new user join message
func NewJoinMessage(userid, roomid int64, username string) Message {
	msg := newMessage(roomid, MessageTypeJoin)
	msg.UserID = userid
	msg.Username = username
	return msg
}

// NewLeaveMessage creates a new user Leave message for live stream room
func NewLeaveMessage(userid, roomid int64, username string) Message {
	msg := newMessage(roomid, MessageTypeLeave)
	msg.UserID = userid
	msg.Username = username
	return msg
}

// NewSystemMessage create a system message
func NewSystemMessage(roomid int64, Content string) Message {
	msg := newMessage(roomid, MessageTypeSystem)
	msg.Content = Content
	return msg
}

// NewErrorMessage create a error message
func NewErrorMessage(roomid int64, Content string) Message {
	msg := newMessage(roomid, MessageTypeError)
	msg.Content = Content
	return msg
}

// NewHeartBeatMessage create a heartbeat message
func NewHeartBeatMessage(roomID int64) Message {
	return newMessage(roomID, MessageTypeHeartBeat)
}

// NewOnlineCountMessage create a online count message
func NewOnlineCountMessage(roomID int64, count int) Message {
	msg := newMessage(roomID, MessageTypeOnlineCount)
	msg.Content = fmt.Sprintf("%d", count)
	return msg
}

func NewLikeMessage(roomId int64, userId int64, username string) Message {
	msg := newMessage(roomId, MessageTypeLike)
	msg.UserID = userId
	msg.Username = username
	msg.Content = fmt.Sprintf("user: %s liked the stream ", username)
	return msg
}

func NewLikeMessageCount(roomId int64, count int64) Message {
	msg := newMessage(roomId, MessageTypeLikeCount)
	msg.Content = fmt.Sprintf("%d", count)
	return msg
}

func saveMessageAsyn(msgService *service.MsgService, msg Message) {
	if msgService == nil {
		log.Printf("skip saving websocket message : message service is nil, type = %q", msg.Type)
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err := msgService.SaveMsg(ctx, msg.RoomID, msg.UserID, msg.Username, msg.Content, msg.Type)

		if err != nil {
			log.Printf("fail to save message to db (room=%d, user=%d): %v",
				msg.RoomID, msg.UserID, err)
		}
	}()
}
