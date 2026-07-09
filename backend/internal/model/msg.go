package model

import "github.com/Sheepc123/golang-live-stream/internal/model/entity"

type MsgResponse struct {
	ID        int64  `json:"id"`
	RoomID    int64  `json:"room_id"`
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
}

type MessageListResponse struct {
	Messages []MsgResponse `json:"messages"`
	Total    int           `json:"total"`
}

func NewMsgResponse(m *entity.Message) MsgResponse {
	return MsgResponse{
		ID:        m.ID,
		RoomID:    m.RoomID,
		UserID:    m.UserID,
		Username:  m.Username,
		Content:   m.Content,
		Type:      m.Type,
		Timestamp: m.CreatedAt.Unix(),
	}
}
