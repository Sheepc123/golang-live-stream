package model

import (
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
)



// RoomResponse represents the response for frontend
type RoomResponse struct {
	ID          int64  `json:"id"`
	OwnerID     int64  `json:"owner_id"`
	Title       string `json:"title"`
	ChannelName string `json:"anchor_name"`
	Category    string `json:"category"`
	CoverURL    string `json:"cover_url"`
	StreamURL   string `json:"stream_url"`
	Description string `json:"description"`
	Status      string `json:"status"`
	ViewerCount int64  `json:"viewer_count"`
	CreatedAt   string `json:"created_at"`
}

// RoomList represents the all available live rooms.
type RoomListResponse struct {
	Rooms []RoomResponse `json:"rooms"`
	Total int            `json:"total"`
}

func NewRoomResponse(room *entity.Room) RoomResponse {
	return RoomResponse{
		ID:          room.ID,
		OwnerID:     room.OwnerId,
		Title:       room.Title,
		ChannelName: room.ChannelName,
		Category:    room.Category,
		CoverURL:    room.CoverURL,
		StreamURL:   room.StreamURL,
		Description: room.Description,
		Status:      room.Status,
		ViewerCount: room.ViewerCount,
		CreatedAt:   room.CreatedAt.Format(time.RFC3339),
	}

}

type CreateRoomRequest struct {
	Title       string `json:"title" binding:"required"` // 标题必填
	ChannelName string `json:"anchor_name"`              // json 名与列表接口保持一致（前端字段是 anchor_name）
	Category    string `json:"category"`
	CoverURL    string `json:"cover_url"`
	StreamURL   string `json:"stream_url"`
	Description string `json:"description"`
}

// edit the status of living-room
// Implement the live stream on/off feature
type UpdateRoomRequest struct {
	Title       string `json:"title" binding:"required"`
	ChannelName string `json:"anchor_name"`
	Category    string `json:"category"`
	CoverURL    string `json:"cover_url"`
	StreamURL   string `json:"stream_url"`
	Description string `json:"description"`
	Status      string `json:"status"` // "live" / "offline"
}
