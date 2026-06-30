package service

import (
	"errors"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/model"
)

// 1.Room service 前端请求，返回当前直播页面 初始化直播间假数据
type RoomService struct {
	rooms []model.Room
}

var ErrRoomNotFound = errors.New("room not found")

func NewRoomService() *RoomService {
	now := time.Now()
	return &RoomService{
		rooms: []model.Room{
			{
				ID:          1,
				Title:       "Music Live",
				ChannelName: "Time Music",
				Category:    "Music",
				CoverURL:    "https://picsum.photos/seed/music-live/640/360",
				StreamURL:   "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
				Description: "A relaxing music live channel.",
				Status:      "live",
				ViewerCount: 1280,
				CreatedAt:   now,
			},
			{
				ID:          2,
				Title:       "Game Arena",
				ChannelName: "Time Gaming",
				Category:    "Gaming",
				CoverURL:    "https://picsum.photos/seed/game-arena/640/360",
				StreamURL:   "https://test-streams.mux.dev/test_001/stream.m3u8",
				Description: "Live gameplay and tournament highlights.",
				Status:      "live",
				ViewerCount: 3421,
				CreatedAt:   now,
			},
			{
				ID:          3,
				Title:       "Tech Talk",
				ChannelName: "Time Tech",
				Category:    "Technology",
				CoverURL:    "https://picsum.photos/seed/tech-talk/640/360",
				StreamURL:   "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
				Description: "Tech news, demos, and product talks.",
				Status:      "live",
				ViewerCount: 876,
				CreatedAt:   now,
			},
		},
	}
}

func (s *RoomService) RoomList() ([]model.Room, error) {
	return s.rooms, nil
}

func (s *RoomService) GetRoomByID(ID int64) (*model.Room, error) {
	for i := range s.rooms {
		if s.rooms[i].ID == ID {
			return &s.rooms[i], nil
		}
	}
	return nil, ErrRoomNotFound
}
