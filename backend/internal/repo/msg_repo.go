package repo

import (
	"context"
	"slices"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
)

type MsgRepo interface {
	ListByRoom(ctx context.Context, Id int64, limit int) ([]entity.Message, error)

	Create(ctx context.Context, msg *entity.Message) error
}

type msgRepo struct {
	db *gorm.DB
}

func NewMesRep(db *gorm.DB) MsgRepo {
	return &msgRepo{db: db}
}

func (r *msgRepo) ListByRoom(ctx context.Context, Id int64, limit int) ([]entity.Message, error) {
	var msgs []entity.Message

	err := r.db.WithContext(ctx).Where("room_id = ?", Id).Order("created_at desc").Limit(limit).Find(&msgs).Error

	if err != nil {
		return nil, err
	}
	slices.Reverse(msgs)
	return msgs, err

}

func (r *msgRepo) Create(ctx context.Context, msg *entity.Message) error {
	return r.db.WithContext(ctx).Create(msg).Error
}
