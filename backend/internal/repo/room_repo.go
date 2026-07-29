package repo

import (
	"context"
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
)

var ErrRoomNotFound error = errno.RoomNotFound
var ErrRoomForbidden error = errno.RoomForbidden

type RoomRepo interface {
	RoomList(ctx context.Context) ([]entity.Room, error)
	FindByRoomID(ctx context.Context, id int64) (*entity.Room, error)

	ListMyRoom(ctx context.Context, OwnerID int64) ([]entity.Room, error)
	Create(ctx context.Context, room *entity.Room) error
	Delete(ctx context.Context, id int64) error

	UpdateStatus(ctx context.Context, roomId int64, status string) error
	UpdateProfile(ctx context.Context, room *entity.Room) error
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

func (r *roomRepo) ListMyRoom(ctx context.Context, OwnerID int64) ([]entity.Room, error) {
	var myrooms []entity.Room

	err := r.db.WithContext(ctx).Where("owner_id = ?", OwnerID).Order("id desc").Find(&myrooms).Error

	if err != nil {
		return nil, err
	}
	return myrooms, nil
}

func (r *roomRepo) Create(ctx context.Context, room *entity.Room) error {

	return r.db.WithContext(ctx).Create(room).Error
}

func (r *roomRepo) Delete(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Delete(&entity.Room{}, id).Error
}

func (r *roomRepo) UpdateStatus(ctx context.Context, roomId int64, status string) error {
	res := r.db.WithContext(ctx).Model(&entity.Room{}).
		Where("id = ?", roomId).
		Update("status", status)

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrRoomNotFound
	}
	return nil
}

func (r *roomRepo) UpdateProfile(ctx context.Context, room *entity.Room) error {
	res := r.db.WithContext(ctx).Model(&entity.Room{}).
		Where("id = ?", room.ID).
		Select("title", "channel_name", "category", "cover_url", "stream_url", "description").
		Updates(room)

	if res.Error != nil {
		return res.Error
	}
	return nil
}
