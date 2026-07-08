package service

import (
	"context"
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
)

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
