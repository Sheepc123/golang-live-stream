package live

import (
	"context"
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"go.uber.org/zap"
)

// implement like_count
type Notifier interface {
	NotifyLikeCount(roomId int64, count int64)
}

type LiveService struct {
	SMgr     *SessionManager
	roomRepo repo.RoomRepo
	notifier Notifier
}

func NewLiveService(SMgr *SessionManager, RoomRpo repo.RoomRepo, notifier Notifier) *LiveService {
	return &LiveService{SMgr: SMgr, roomRepo: RoomRpo, notifier: notifier}
}

// LiveStart start the live and return the SessionId and error
func (s *LiveService) LiveStart(ctx context.Context, roomId int64, OwnerId int64) (int64, error) {
	room, err := s.roomRepo.FindByRoomID(ctx, roomId)

	if err != nil {
		return 0, err
	}

	if room.OwnerId != OwnerId {
		return 0, repo.ErrRoomForbidden
	}

	SId, e := s.SMgr.Open(ctx, room)

	if e != nil {
		return 0, e
	}

	if err := s.roomRepo.UpdateStatus(ctx, roomId, entity.RoomStatusLive); err != nil {
		logger.L().Error("set room live fail",
			zap.Int64("room_id", roomId), zap.Error(err))
	}
	s.notifyLikeCount(ctx, roomId)
	return SId, nil
}

func (s *LiveService) LiveStop(ctx context.Context, roomId int64, OwnerId int64) error {
	room, err := s.roomRepo.FindByRoomID(ctx, roomId)

	if err != nil {
		return err
	}

	if room.OwnerId != OwnerId {
		return repo.ErrRoomForbidden
	}

	if err := s.SMgr.Close(ctx, roomId); err != nil && !errors.Is(err, repo.ErrLiveSessionNotActive) {
		return err
	}

	if err := s.roomRepo.UpdateStatus(ctx, roomId, entity.RoomStatusStop); err != nil {
		return err
	}

	s.notifyLikeCount(ctx, roomId)

	return nil
}

func (s *LiveService) notifyLikeCount(ctx context.Context, roomId int64) {
	if s.notifier == nil {
		return
	}
	likecount, err := s.SMgr.GetLikeCount(ctx, roomId)

	if err != nil {
		logger.L().Error("notify like count fail",
			zap.Int64("room_id", roomId), zap.Error(err))
		return
	}

	s.notifier.NotifyLikeCount(roomId, likecount)
}
