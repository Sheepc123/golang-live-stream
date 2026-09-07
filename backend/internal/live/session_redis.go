package live

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const sessionTTL = 24 * time.Hour

// implements three thing
// Live Open, save the session_id to redis
// Live End, save the like and Online Peak Count,delete the session_id from redis
// Current
// ResolveId Reliable ResolveID retrieval: query DB and self-heal on cache miss.

type RoomStat struct {
	RoomID      int64
	SessionID   int64 // 0 表示该房间当前未开播
	OnlineCount int64
	LikeCount   int64
}

type SessionManager struct {
	lsRepo        repo.LSRepo
	likeCounter   *LikeCounter
	rdb           *redis.Client
	onlineCounter *OnlineCounter
	cache         *sessionCache
}

func NewSessionManager(lr repo.LSRepo, lc *LikeCounter, rdb *redis.Client, oc *OnlineCounter) *SessionManager {
	return &SessionManager{lsRepo: lr, likeCounter: lc, rdb: rdb, onlineCounter: oc, cache: newSessionCache()}
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
		s.RestoreSID(ctx, room.ID, session.ID)
		return session.ID, nil

	case errors.Is(err, repo.ErrLiveSessionExists):
		cur, e := s.lsRepo.Current(ctx, room.ID)
		if e != nil {
			return 0, e
		}
		if cur == nil {
			return 0, err
		}

		s.RestoreSID(ctx, room.ID, cur.ID)
		return cur.ID, nil
	default:
		return 0, err
	}
}

// read the SessionId from redis
// if returns 0 equals redis not exits.
func (s *SessionManager) currentID(ctx context.Context, roomId int64) (int64, error) {
	SessionId, err := s.rdb.Get(ctx, sessionKey(roomId)).Int64()

	if errors.Is(err, redis.Nil) {
		return 0, nil
	}

	if err != nil {
		logger.L().Error("read session id fail",
			zap.Int64("room_id", roomId),
			zap.Error(err),
		)
		return 0, err
	}
	return SessionId, nil
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

	likeCount, err := s.likeCounter.GetHistoryLike(ctx, sId)
	if err != nil {
		return fmt.Errorf("get like count (room = %d, session = %d): %w", roomId, sId, err)
	}

	peak, err := s.onlineCounter.Peak(ctx, sId)

	if err != nil {
		return fmt.Errorf("get peak (room = %d, session = %d): %w", roomId, sId, err)
	}

	if err := s.lsRepo.SaveSession(ctx, sId, likeCount, peak); err != nil {
		return fmt.Errorf("failed to save session (room = %d ,sessionId = %d):%w", roomId, sId, err)

	}

	if err := s.rdb.Del(ctx, sessionKey(roomId)).Err(); err != nil {
		logger.L().Error("del session key fail",
			zap.Int64("room_id", roomId),
			zap.Error(err),
		)
	}

	s.cache.del(roomId)

	return nil
}

// ResolveId find the Sid and judge whether redis miss or live do not exit.
// if return 0 && err != nil -> the corresponding room is offline.
func (s *SessionManager) ResolveID(ctx context.Context, roomId int64) (int64, error) {
	now := time.Now()
	if sid, ok := s.cache.get(roomId, now); ok {
		return sid, nil
	}

	Sid, err := s.currentID(ctx, roomId)
	if err != nil {
		return 0, err
	}

	if Sid != 0 {
		s.cache.set(roomId, Sid, now)
		return Sid, nil
	}

	cur, err := s.lsRepo.Current(ctx, roomId)

	if err != nil {
		return 0, err
	}

	// do not exist the live session
	if cur == nil {
		s.cache.set(roomId, 0, now)
		return 0, nil
	}

	// repo has the session but redis do not have redis miss.
	s.RestoreSID(ctx, roomId, cur.ID)
	s.cache.set(roomId, cur.ID, now)
	return cur.ID, nil

}

// RestoreSID restore the live session redis.
func (s *SessionManager) RestoreSID(ctx context.Context, roomId int64, SId int64) {
	err := s.rdb.Set(ctx, sessionKey(roomId), SId, sessionTTL).Err()

	if err != nil {
		logger.L().Error("cache session fail",
			zap.Int64("room_id", roomId),
			zap.Int64("session_id", SId),
			zap.Error(err),
		)
	}
	s.cache.set(roomId, SId, time.Now())
}

func (s *SessionManager) GetLikeCount(ctx context.Context, roomId int64) (int64, error) {
	sId, err := s.ResolveID(ctx, roomId)

	if err != nil {
		logger.L().Error("resolve session for like count fail",
			zap.Int64("room_id", roomId),
			zap.Error(err),
		)
		return 0, err
	}
	if sId == 0 {
		return 0, nil
	}
	return s.likeCounter.GetHistoryLike(ctx, sId)
}

func (s *SessionManager) ViewerJoin(ctx context.Context, roomId int64, userId int64) int64 {
	n := s.onlineCounter.Join(ctx, roomId, userId)

	sid, err := s.ResolveID(ctx, roomId)

	if sid != 0 && err == nil {
		s.onlineCounter.UpdatePeak(ctx, sid, n)
	}
	return n
}

func (s *SessionManager) ViewerLeave(ctx context.Context, roomId, userId int64) int64 {
	return s.onlineCounter.Leave(ctx, roomId, userId)
}

func (s *SessionManager) OnlineCount(ctx context.Context, roomId int64) (int64, error) {
	return s.onlineCounter.Count(ctx, roomId)
}


// get the RoomStat by using redis pipeline 
func (s *SessionManager) BatchStats(ctx context.Context, roomIDs []int64) ([]RoomStat, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}

	sids := make([]int64, len(roomIDs))
	for i, roomId := range roomIDs {
		sid, err := s.ResolveID(ctx, roomId)
		if err != nil {
			// -1 是哨兵值:「这一轮跳过这个房间」。
			// 不能用 0 —— 0 已经有明确含义(未开播),两者不能混用。
			sids[i] = -1
			continue
		}
		sids[i] = sid
	}

	// 第二步:一次 Pipeline 打包所有读命令。
	pipe := s.rdb.Pipeline()

	onlineCmds := make([]*redis.IntCmd, len(roomIDs))
	likeCmds := make([]*redis.StringCmd, len(roomIDs))

	for i, roomId := range roomIDs {
		if sids[i] < 0 {
			continue
		}
		onlineCmds[i] = pipe.HLen(ctx, ViewersKey(roomId))

		// 未开播的房间没有点赞 key,不用白发一条命令。
		if sids[i] > 0 {
			likeCmds[i] = pipe.Get(ctx, LikeKey(sids[i]))
		}
	}

	// Pipeline 里只要有一条命令返回 redis.Nil(key 不存在),
	// Exec 就会把它当成整体的 err 返回。但 key 不存在完全正常
	// (刚开播还没人点赞),必须放行 —— 只有真正的连接/协议错误才算失败。
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	// 第三步:取结果。命令早就执行完了,这里只是读内存里的返回值。
	stats := make([]RoomStat, 0, len(roomIDs))

	for i, roomId := range roomIDs {
		if onlineCmds[i] == nil {
			continue
		}

		// HLEN 对不存在的 key 返回 0 而不是 Nil,所以这里出错就是真出错。
		online, err := onlineCmds[i].Result()
		if err != nil {
			continue
		}

		var like int64
		if likeCmds[i] != nil {
			n, err := likeCmds[i].Int64()
			switch {
			case err == nil:
				like = n
			case errors.Is(err, redis.Nil):
				// 这场还没有人点赞,按 0 处理
			default:
				continue
			}
		}

		stats = append(stats, RoomStat{
			RoomID:      roomId,
			SessionID:   sids[i],
			OnlineCount: online,
			LikeCount:   like,
		})
	}

	return stats, nil
}

// EvictExpiredSessions 清理本地 session 缓存的过期条目,由 Aggregator 每秒调用。
func (s *SessionManager) EvictExpiredSessions() {
	s.cache.evictSCache(time.Now())
}
