package repo

import (
	"context"
	"errors"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrLiveSessionExists    error = errno.LiveAlreadyStarted
	ErrLiveSessionNotActive error = errno.LiveNotActive
)

type LSRepo interface {
	// SaveSession save likeCount and peak online when a live_stream is over.
	// ErrLiveSessionNotActive means the database do not have the live session
	SaveSession(ctx context.Context, sessionId, likeCount, peakCount int64) error

	// Find the LiveSession by roomId through Database.
	// If return nil,nil it means do not find the corresponding live_session Record .
	Current(ctx context.Context, roomId int64) (*entity.LiveSession, error)

	// CreateSession creates a new session when a new live_stream is created.
	// return ErrLiveSessionExists eror
	CreateSession(ctx context.Context, roomId int64) (*entity.LiveSession, error)
}

type lsRepo struct {
	db *gorm.DB
}

func NewLiveSessionRepo(db *gorm.DB) LSRepo {
	return &lsRepo{db: db}
}

func (r *lsRepo) CreateSession(ctx context.Context, roomId int64) (*entity.LiveSession, error) {
	var session *entity.LiveSession

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lockedRoom entity.Room
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&lockedRoom, roomId).Error

		if err != nil {
			return err
		}

		// Judge whether the live_session already session
		var count int64
		err = tx.Model(&entity.LiveSession{}).Where("room_id = ? AND ended_at IS NULL", roomId).Count(&count).Error

		if err != nil {
			return err
		}

		if count > 0 {
			return ErrLiveSessionExists
		}

		session = &entity.LiveSession{
			RoomID:    lockedRoom.ID,
			StartedAt: time.Now(),
			LiveTitle: lockedRoom.Title,
		}

		return tx.Create(session).Error

	})

	if err != nil {
		return nil, err
	}
	return session, nil
}

func (r *lsRepo) SaveSession(ctx context.Context, sessionId, likeCount, peakcount int64) error {

	res := r.db.WithContext(ctx).Model(&entity.LiveSession{}).Where("id = ? AND ended_at IS NULL", sessionId).Updates(
		map[string]any{
			"ended_at":          time.Now(),
			"like_count":        likeCount,
			"peak_online_count": peakcount,
		})

	if res.Error != nil {
		return res.Error
	}

	if res.RowsAffected == 0 {
		return ErrLiveSessionNotActive
	}
	return nil
}

// Find the LiveSession through roomId.
// If return nil,nil it means do not find the corresponding live_session Record .
func (r *lsRepo) Current(ctx context.Context, roomId int64) (*entity.LiveSession, error) {
	var s entity.LiveSession
	err := r.db.WithContext(ctx).Where("room_id = ? AND ended_at IS NULL", roomId).First(&s).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	return &s, nil
}
