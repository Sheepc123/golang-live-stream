package repo

import (
	"context"
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
)

var ErrRoomNotFound = errors.New("room not found")

type RoomRepo interface {
	RoomList(ctx context.Context) ([]entity.Room, error)
	FindByRoomID(ctx context.Context, id int64) (*entity.Room, error)
}

type roomRepo struct {
	db *gorm.DB
}

func NewRoomRepo(db *gorm.DB) RoomRepo {
	return &roomRepo{db: db}
}

// List function get the all rooms
func (r *roomRepo) RoomList(ctx context.Context) ([]entity.Room, error) {
	var rooms []entity.Room
	err := r.db.WithContext(ctx).Order("id asc").Find(&rooms).Error

	if err != nil {
		return nil, err
	}

	return rooms, nil
}

// FindByRoomId get room through ID
func (r *roomRepo) FindByRoomID(ctx context.Context, id int64) (*entity.Room, error) {
	var room entity.Room

	err := r.db.WithContext(ctx).First(&room, id).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRoomNotFound
		}
		return nil, err
	}
	return &room, nil
}
