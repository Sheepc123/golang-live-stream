package live

import (
	"context"
	"errors"
	"log"

	"github.com/Sheepc123/golang-live-stream/internal/repo"
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

	room.Status = "live"

	if err := s.roomRepo.Update(ctx, room); err != nil {
		log.Printf("set room live fail (room=%d): %v", roomId, err)
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

	room.Status = "offline"

	if err := s.roomRepo.Update(ctx, room); err != nil {
		return err
	}

	s.notifyLikeCount(ctx, roomId)

	return nil
}

func (s *LiveService) notifyLikeCount(ctx context.Context, roomId int64) {
	if s.notifier == nil {
		return
	}
	s.notifier.NotifyLikeCount(roomId, s.SMgr.GetLikeCount(ctx, roomId))
}
