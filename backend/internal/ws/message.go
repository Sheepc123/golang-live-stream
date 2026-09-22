package ws

import (
	"strconv"
	"time"
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
	MessageTypeRoomEvent   = "room_event"
)

// Message represents the message in websocket.
type Message struct {
	Type          string `json:"type"`
	RoomID        int64  `json:"room_id"`
	UserID        int64  `json:"user_id"`
	Username      string `json:"username"`
	Content       string `json:"content"`
	Timestamp     int64  `json:"timestamp"`
	LiveSessionID int64  `json:"live_session_id"`

	// 以下四个字段只在 room_event(每秒聚合的进出场名单)里出现。
	//
	// omitempty 不是为了好看:这四个字段存在于每一条下行消息的结构体里,
	// 但只有 room_event 用得上。没有 omitempty 的话,每条弹幕都会白带
	// `"joined":null,"left":null,"joined_more":0,"left_more":0`
	// —— 五十来个字节 × 6 万连接 × 每秒几十条 = 几十 MB/s 的纯浪费。
	//
	// 扇出路径上的每一个字节都要乘以连接数,这是和平时写 CRUD 完全不同的成本模型。
	Joined     []string `json:"joined,omitempty"`
	Left       []string `json:"left,omitempty"`
	JoinedMore int      `json:"joined_more,omitempty"`
	LeftMore   int      `json:"left_more,omitempty"`
}

// newMessage is the local-level constructor reponsible for populating
// the public fields such as RoomID,Type
func newMessage(roomId int64, Type string) Message {
	return Message{
		RoomID:    roomId,
		Type:      Type,
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewChatMessage creates a new Chat message
func NewChatMessage(userId, roomid int64, username, Content string, sId int64) Message {
	msg := newMessage(roomid, MessageTypeChat)
	msg.UserID = userId
	msg.Content = Content
	msg.Username = username
	msg.LiveSessionID = sId
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
func NewOnlineCountMessage(roomID int64, count int64) Message {
	msg := newMessage(roomID, MessageTypeOnlineCount)
	msg.Content = strconv.FormatInt(count, 10)
	return msg
}

// 这里原来有一个 NewLikeMessage(单条「XX 点了个赞」)。
// III.4 把点赞改成聚合推送后,LikeAction 只做 Redis INCR、
// 只有 like_count 会下行,它就再没有调用者了 —— 已删除。
//
// 前端 DanmakuList 仍然能渲染 type="like",那是刻意留着的:
// 万一以后要做「点赞飘心特效」还得靠它,而一个用不到的 case 分支
// 在前端是零成本的。后端留一个没人调的构造函数就不一样了 ——
// 下一个人会以为它还在链路上,照着它去推断行为。

func NewLikeMessageCount(roomId int64, count int64) Message {
	msg := newMessage(roomId, MessageTypeLikeCount)
	msg.Content = strconv.FormatInt(count, 10)
	return msg
}

// newRoomEventMessage 把一轮积累的进出场事件打包成一条消息。
func newRoomEventMessage(roomId int64, e *roomEvents) Message {
	msg := newMessage(roomId, MessageTypeRoomEvent)
	msg.Joined = e.joined
	msg.Left = e.left
	msg.JoinedMore = e.joinedOverflow
	msg.LeftMore = e.leftOverflow
	return msg
}

// NewJoinMessage / NewLeaveMessage 只在实验 C(即时广播)模式下使用。
// 生产路径走 room_event 聚合,不会逐条发这两种消息。
// 前端 DanmakuList 的 formatMessageContent 早就能渲染 join/leave 类型。
func NewJoinMessage(roomId, userId int64, username string) Message {
	msg := newMessage(roomId, MessageTypeJoin)
	msg.UserID = userId
	msg.Username = username
	return msg
}

func NewLeaveMessage(roomId, userId int64, username string) Message {
	msg := newMessage(roomId, MessageTypeLeave)
	msg.UserID = userId
	msg.Username = username
	return msg
}
