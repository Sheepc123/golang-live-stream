package repo

import (
	"context"
	"slices"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MsgRepo interface {
	ListBySessionID(ctx context.Context, roomId, sessionId int64, limit int) ([]entity.Message, error)
	Create(ctx context.Context, msg *entity.Message) error
	CreateIfAbsent(ctx context.Context, msg *entity.Message) error
}

type msgRepo struct {
	db *gorm.DB
}

func NewMesRep(db *gorm.DB) MsgRepo {
	return &msgRepo{db: db}
}

// List Message by SessionID
func (r *msgRepo) ListBySessionID(ctx context.Context, roomId, sessionId int64, limit int) ([]entity.Message, error) {
	var msgs []entity.Message

	err := r.db.WithContext(ctx).Where("room_id = ? AND live_session_id = ?", roomId, sessionId).Order("sent_at DESC, id DESC").Limit(limit).Find(&msgs).Error

	if err != nil {
		return nil, err
	}
	slices.Reverse(msgs)
	return msgs, nil

}

func (r *msgRepo) Create(ctx context.Context, msg *entity.Message) error {
	return r.db.WithContext(ctx).Create(msg).Error
}

func (r *msgRepo) CreateIfAbsent(ctx context.Context, msg *entity.Message) error {
	return r.db.WithContext(ctx).Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "event_id"}},
			DoNothing: true,
		}).Create(msg).Error
}
