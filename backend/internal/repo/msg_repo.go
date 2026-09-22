package repo

import (
	"context"
	"slices"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Limit the number of rows in each INSERT statement.
const messageInsertBatchSize = 500

type MsgRepo interface {
	ListBySessionID(ctx context.Context, roomId, sessionId int64, limit int) ([]entity.Message, error)
	CreateBatchIfAbsent(ctx context.Context, msgs []entity.Message) error
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



func (r *msgRepo) CreateBatchIfAbsent(ctx context.Context, msgs []entity.Message) error {
	// An empty batch requires no database work.
	if len(msgs) == 0 {
		return nil
	}

	return r.db.WithContext(ctx).
		// Let MySQL generate primary keys on every attempt.
		// GORM may populate IDs in the input slice during an earlier attempt.
		Omit("ID").
		Clauses(clause.OnConflict{
			// Deduplication relies on the unique event_id index.
			Columns: []clause.Column{{Name: "event_id"}},
			DoNothing: true,
		}).
		CreateInBatches(&msgs, messageInsertBatchSize).
		Error
}
