package live

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"github.com/redis/go-redis/v9"
)

const sessionTTL = 24 * time.Hour

// implements three thing
// Live Open, save the session_id to redis
// Live End, save the like and Online Peak Count,delete the session_id from redis
// Current
// ResolveId Reliable ResolveID retrieval: query DB and self-heal on cache miss.

type SessionManager struct {
	lsRepo        repo.LSRepo
	likeCounter   *LikeCounter
	rdb           *redis.Client
	onlineCounter *OnlineCounter
}

func NewSessionManager(lr repo.LSRepo, lc *LikeCounter, rdb *redis.Client, oc *OnlineCounter) *SessionManager {
	return &SessionManager{lsRepo: lr, likeCounter: lc, rdb: rdb, onlineCounter: oc}
}

func sessionKey(roomId int64) string {
	return fmt.Sprintf("room:%d:session", roomId)
}

// Open the Live create save SessionId to redis
// return sessionId and Error
func (s *SessionManager) Open(ctx context.Context, room *entity.Room) (int64, error) {
	session, err := s.lsRepo.CreateSession(ctx, room.ID)

	switch {
	case err == nil:
		s.cacheID(ctx, room.ID, session.ID)
		return session.ID, nil

	case errors.Is(err, repo.ErrLiveSessionExists):
		cur, e := s.lsRepo.Current(ctx, room.ID)
		if e != nil {
			return 0, e
		}
		if cur == nil {
			return 0, err
		}

		s.cacheID(ctx, room.ID, cur.ID)
		return cur.ID, nil
	default:
		return 0, err
	}
}

// read the SessionId from redis
func (s *SessionManager) CurrentID(ctx context.Context, roomId int64) int64 {
	SessionId, err := s.rdb.Get(ctx, sessionKey(roomId)).Int64()
	if err == redis.Nil {
		return 0
	}

	if err != nil {
		log.Printf("read session id fail (room = %d) : %v", roomId, err)
		return 0
	}
	return SessionId
}

// Close function close the room and save LikeCount and Peak Online Count Session.
func (s *SessionManager) Close(ctx context.Context, roomId int64) error {
	sId, err := s.ResolveID(ctx, roomId)
	if err != nil {
		return fmt.Errorf("resolve session (room = %d): %w", roomId, err)
	}

	if sId == 0 {
		return repo.ErrLiveSessionNotActive
	}

	likeCount := s.likeCounter.GetHistoryLike(ctx, sId)

	peak := s.onlineCounter.Peak(ctx, sId)

	if err := s.lsRepo.SaveSession(ctx, sId, likeCount, peak); err != nil {
		return fmt.Errorf("failed to save session (room = %d ,sessionId = %d):%w", roomId, sId, err)

	}

	if err := s.rdb.Del(ctx, sessionKey(roomId)).Err(); err != nil {
		log.Printf("del session key fail (room = %d) : %v", roomId, err)
	}

	return nil
}

// ResloveId find the Sid and judge whether redis miss or live do not exit
func (s *SessionManager) ResolveID(ctx context.Context, roomId int64) (int64, error) {
	if Sid := s.CurrentID(ctx, roomId); Sid != 0 {
		return Sid, nil
	}

	cur, err := s.lsRepo.Current(ctx, roomId)

	if err != nil {
		return 0, err
	}

	// do not exist the live session
	if cur == nil {
		return 0, nil
	}

	// repo has the session but redis do not have redis miss.

	s.cacheID(ctx, roomId, cur.ID)
	return cur.ID, nil

}

// cacheID restore the live session redis.
func (s *SessionManager) cacheID(ctx context.Context, roomId int64, SId int64) {
	err := s.rdb.Set(ctx, sessionKey(roomId), SId, sessionTTL).Err()

	if err != nil {
		log.Printf("cache session fail (room = %d,session = %d): %v", roomId, SId, err)
	}
}

func (s *SessionManager) GetLikeCount(ctx context.Context, roomId int64) int64 {
	sId, err := s.ResolveID(ctx, roomId)

	if err != nil {
		log.Printf("resolve session for like count fail(room = %d):%v", roomId, err)
		return 0
	}
	if sId == 0 {
		return 0
	}
	return s.likeCounter.GetHistoryLike(ctx, sId)
}

func (s *SessionManager) ViewerJoin(ctx context.Context, roomId int64, userId int64) int64 {
	n := s.onlineCounter.Join(ctx, roomId, userId)
	if sid := s.CurrentID(ctx, roomId); sid != 0 {
		s.onlineCounter.UpdatePeak(ctx, sid, n)
	}
	return n
}

func (s *SessionManager) ViewerLeave(ctx context.Context, roomId, userId int64) int64 {
	return s.onlineCounter.Leave(ctx, roomId, userId)
}

func (s *SessionManager) OnlineCount(ctx context.Context, roomId int64) int64 {
	return s.onlineCounter.Count(ctx, roomId)
}
