package service

import (
	"context"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
)

const (
	defaultMessageLimit = 50
	maxMessageLimit     = 200
)

// Msg Service
// 1. Read history message ID from MsgRepo interface
// 2.Get
type MsgService struct {
	msgRepo repo.MsgRepo
	// JWT     config.JWTConfig
}

func NewMsgService(repo repo.MsgRepo) *MsgService {
	return &MsgService{
		msgRepo: repo,
	}
}

// get history message through SessionId
func (s MsgService) GetHistoryMessage(ctx context.Context, roomId, SessionId int64, limit int) ([]entity.Message, error) {
	if limit <= 0 {
		limit = defaultMessageLimit
	}
	if limit > maxMessageLimit {
		limit = maxMessageLimit
	}

	return s.msgRepo.ListBySessionID(ctx, roomId, SessionId, limit)
}

// Savemessage save the message to database using repo.Create funciton
func (s MsgService) SaveMsg(ctx context.Context, RoomID int64, UserId int64, Username string, content string, msgType string) error {
	msg := &entity.Message{
		RoomID:   RoomID,
		UserID:   UserId,
		Username: Username,
		Content:  content,
		Type:     msgType,
	}
	return s.msgRepo.Create(ctx, msg)
}
