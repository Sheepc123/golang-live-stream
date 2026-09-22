package service

import (
	"context"

	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
)

type OnlineCounts interface {
	Count(ctx context.Context, roomID int64) (int64, error)
	BatchCount(ctx context.Context, roomIDs []int64) (map[int64]int64, error)
}

// 1.Room service
type RoomService struct {
	roomRepo repo.RoomRepo
	online   OnlineCounts
}

func NewRoomService(r repo.RoomRepo, online OnlineCounts) *RoomService {
	return &RoomService{roomRepo: r, online: online}
}

func (s *RoomService) RoomList(ctx context.Context) ([]entity.Room, map[int64]int64, error) {
	rooms, err := s.roomRepo.RoomList(ctx)

	if err != nil {
		return nil, nil, err
	}
	return rooms, s.onlineFor(ctx, rooms), nil
}

func (s *RoomService) GetRoomByID(ctx context.Context, id int64) (*entity.Room, int64, error) {
	room, err := s.roomRepo.FindByRoomID(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	n, err := s.online.Count(ctx, id)
	if err != nil {
		n = 0
	}
	return room, n, nil
}

func (s *RoomService) ListMyRoom(ctx context.Context, ownerId int64) ([]entity.Room, map[int64]int64, error) {

	rooms, err := s.roomRepo.ListMyRoom(ctx, ownerId)

	if err != nil {
		return nil, nil, err
	}

	return rooms, s.onlineFor(ctx, rooms), nil
}

func (s *RoomService) Create(ctx context.Context, ownerId int64, req *model.CreateRoomRequest) (*entity.Room, error) {
	room := &entity.Room{
		OwnerId:     ownerId,
		Title:       req.Title,
		ChannelName: req.ChannelName,
		Category:    req.Category,
		CoverURL:    req.CoverURL,
		StreamURL:   req.StreamURL,
		Description: req.Description,
		Status:      "offline", // 新房间默认未开播
	}
	if err := s.roomRepo.Create(ctx, room); err != nil {
		return nil, err
	}
	return room, nil
}

func (s *RoomService) UpdateRoom(ctx context.Context, ownerID int64, roomID int64, req *model.UpdateRoomRequest) (*entity.Room, int64, error) {
	room, err := s.roomRepo.FindByRoomID(ctx, roomID)
	if err != nil {
		return nil, 0, err
	}

	if ownerID != room.OwnerId {
		return nil, 0, repo.ErrRoomForbidden
	}

	room.Title = req.Title
	room.ChannelName = req.ChannelName
	room.Category = req.Category
	room.CoverURL = req.CoverURL
	room.StreamURL = req.StreamURL
	room.Description = req.Description

	// 4. 落库
	if err := s.roomRepo.UpdateProfile(ctx, room); err != nil {
		return nil, 0, err
	}

	n, err := s.online.Count(ctx, roomID)
	if err != nil {
		n = 0
	}

	return room, n, nil
}

func (s *RoomService) DeleteRoom(ctx context.Context, ownerId int64, roomId int64) error {
	room, err := s.roomRepo.FindByRoomID(ctx, roomId)

	if err != nil {
		return err
	}

	if room.OwnerId != ownerId {
		return repo.ErrRoomForbidden
	}

	return s.roomRepo.Delete(ctx, roomId)
}

func (s *RoomService) onlineFor(ctx context.Context, rooms []entity.Room) map[int64]int64 {
	if len(rooms) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(rooms))
	for i := range rooms {
		ids = append(ids, rooms[i].ID)
	}
	counts, err := s.online.BatchCount(ctx, ids)
	if err != nil {
		return nil // 优雅降级:列表照常返回,人数显示 0
	}
	return counts
}
