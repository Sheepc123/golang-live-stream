package model

import "time"

// Room represents the internal live room entity
type Room struct {
	ID          int64
	Title       string
	ChannelName string
	Category    string
	CoverURL    string
	StreamURL   string
	Description string
	Status      string
	ViewerCount int64
	CreatedAt   time.Time
	UpdateAt    time.Time
}

// RoomResponse represents the response for frontend
type RoomResponse struct {
	ID          int64  `json:"id"`
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

func NewRoomResponse(room *Room) RoomResponse {
	return RoomResponse{
		ID:          room.ID,
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
