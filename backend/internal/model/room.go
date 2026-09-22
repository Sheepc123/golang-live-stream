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

func NewRoomResponse(room *entity.Room, ViewerCount int64) RoomResponse {
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
		ViewerCount: ViewerCount,
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

// UpdateRoomRequest 只承载「房间资料」,不含 status。
//
// 直播状态由 POST /rooms/:id/live/start|stop 控制 —— 那两个接口
// 还要建场次、重置/快照点赞数,不是单纯改一个字段。
// 这里原来有一个 Status 字段,但 RoomService.UpdateRoom 从来没读过它:
// 前端传什么都不生效。这种「看起来能用其实被忽略」的字段比缺字段更危险,
// 它会让人写出一个不报错也不生效的调用,然后查半天。已删除。
type UpdateRoomRequest struct {
	Title       string `json:"title" binding:"required"`
	ChannelName string `json:"anchor_name"`
	Category    string `json:"category"`
	CoverURL    string `json:"cover_url"`
	StreamURL   string `json:"stream_url"`
	Description string `json:"description"`
}
