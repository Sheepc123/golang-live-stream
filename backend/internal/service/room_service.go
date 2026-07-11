package service

import (
	"context"
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
)

var ErrRoomForbidden = errors.New("Error permission denied.")

// 1.Room service
type RoomService struct {
	roomRepo repo.RoomRepo
}

var ErrRoomNotFound = errors.New("room not found")

func NewRoomService(r repo.RoomRepo) *RoomService {
	return &RoomService{roomRepo: r}
}

func (s *RoomService) RoomList(ctx context.Context) ([]entity.Room, error) {

	return s.roomRepo.RoomList(ctx)
}

func (s *RoomService) GetRoomByID(ctx context.Context, id int64) (*entity.Room, error) {
	return s.roomRepo.FindByRoomID(ctx, id)
}

func (s *RoomService) ListMyRoom(ctx context.Context, ownerId int64) ([]entity.Room, error) {

	return s.roomRepo.ListMyRoom(ctx, ownerId)
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
		ViewerCount: 0,
	}
	err := s.roomRepo.Create(ctx, room)
	if err != nil {
		return nil, err
	}
	return room, nil
}

func (s *RoomService) UpdateRoom(ctx context.Context, ownerID int64, roomID int64, req *model.UpdateRoomRequest) (*entity.Room, error) {
	room, err := s.roomRepo.FindByRoomID(ctx, roomID)
	if err != nil {
		return nil, err
	}

	if ownerID != room.ID {
		return nil, ErrRoomForbidden
	}

	room.Title = req.Title
	room.ChannelName = req.ChannelName
	room.Category = req.Category
	room.CoverURL = req.CoverURL
	room.StreamURL = req.StreamURL
	room.Description = req.Description
	room.Status = req.Status

	// 4. 落库
	if err := s.roomRepo.Update(ctx, room); err != nil {
		return nil, err
	}
	return room, nil
}

func (s *RoomService) DeleteRoom(ctx context.Context, ownerId int64, roomId int64) error {
	room, err := s.roomRepo.FindByRoomID(ctx, roomId)

	if err != nil {
		return err
	}

	if room.OwnerId != ownerId {
		return ErrRoomForbidden
	}

	return s.DeleteRoom(ctx, ownerId, roomId)
}
