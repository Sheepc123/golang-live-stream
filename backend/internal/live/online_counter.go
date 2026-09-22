package live

/*
	onlineCounter tracks the number of viewers in a room using Redis
	and records the peak viewer count for each live session.
*/
import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const onlineTTL = 24 * time.Hour

type OnlineCounter struct {
	rdb *redis.Client
}

func NewOnlineCounter(rdb *redis.Client) *OnlineCounter {
	return &OnlineCounter{rdb: rdb}
}

// redis key e.g. live:room:5:viewers
func peakKey(SessionId int64) string {
	return fmt.Sprintf("live:session:%d:peak", SessionId)
}

func ViewersKey(roomId int64) string {
	return fmt.Sprintf("live:room:%d:viewers", roomId)
}

// HINCRBY update conneciton count for specified user.
// HLEN returns the total number of unique users.
var joinScript = redis.NewScript(`
	redis.call('HINCRBY',KEYS[1],ARGV[1],1)
	redis.call('EXPIRE',KEYS[1],ARGV[2])
	return redis.call('HLEN', KEYS[1])
`)

func (c *OnlineCounter) Join(ctx context.Context, roomId, userId int64) int64 {
	n, err := joinScript.Run(
		ctx,
		c.rdb,
		[]string{ViewersKey(roomId)},
		userId,
		int64(onlineTTL.Seconds()),
	).Int64()

	if err != nil {
		logger.L().Error("online join fail",
			zap.Int64("room_id", roomId), zap.Int64("user_id", userId), zap.Error(err),
		)
		return 0
	}
	return n
}

var leaveScript = redis.NewScript(`
	local n = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
	if n <= 0 then
	redis.call('HDEL', KEYS[1], ARGV[1])
	end
	if redis.call('EXISTS', KEYS[1]) == 1 then
	redis.call('EXPIRE', KEYS[1], ARGV[2])
	end
	return redis.call('HLEN', KEYS[1])
`)

func (c *OnlineCounter) Leave(ctx context.Context, roomId, userId int64) int64 {
	n, err := leaveScript.Run(
		ctx,
		c.rdb,
		[]string{ViewersKey(roomId)},
		userId,
		int64(onlineTTL.Seconds()),
	).Int64()

	if err != nil {
		logger.L().Error("online leave fail",
			zap.Int64("room_id", roomId), zap.Int64("user_id", userId), zap.Error(err),
		)
		return 0
	}
	return n
}

func (c *OnlineCounter) Count(ctx context.Context, roomId int64) (int64, error) {
	n, err := c.rdb.HLen(ctx, ViewersKey(roomId)).Result()

	if errors.Is(err, redis.Nil) {
		return 0, nil
	}

	if err != nil {
		logger.L().Error("online count fail",
			zap.Int64("room_id", roomId), zap.Error(err),
		)
		return 0, err
	}

	return n, nil
}

var PeakScript = redis.NewScript(`
	local cur = tonumber(redis.call('GET',KEYS[1]) or '0')
	local new = tonumber(ARGV[1])

	if new > cur then 
		redis.call('SET',KEYS[1], new)
	end
	redis.call('EXPIRE', KEYS[1], ARGV[2])
	return redis.call('GET',KEYS[1])
`)

func (c *OnlineCounter) UpdatePeak(ctx context.Context, sessionId, n int64) {
	if sessionId == 0 || n <= 0 {
		return
	}

	err := PeakScript.Run(
		ctx,
		c.rdb,
		[]string{peakKey(sessionId)}, n, int64(onlineTTL.Seconds()),
	).Err()

	if err != nil {
		logger.L().Error("update peak fail",
			zap.Int64("session_id", sessionId), zap.Error(err),
		)
	}
}

func (c *OnlineCounter) Peak(ctx context.Context, sessionId int64) (int64, error) {
	n, err := c.rdb.Get(ctx, peakKey(sessionId)).Int64()

	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		logger.L().Error("get peak fail",
			zap.Int64("session_id", sessionId), zap.Error(err),
		)
		return 0, err
	}
	return n, nil
}


// BatchCount pipeline get the online counts from multiply rooms
func (c *OnlineCounter) BatchCount(ctx context.Context, roomIDs []int64) (map[int64]int64, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(roomIDs))

	for i, roomID := range roomIDs {
		cmds[i] = pipe.HLen(ctx, ViewersKey(roomID))
	}

	// if key is none, HLEN returns the 0 instead of nil.
	// redis.GET
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		logger.L().Error("batch online count fail",
			zap.Int("rooms", len(roomIDs)), zap.Error(err),
		)
		return nil, err
	}

	out := make(map[int64]int64, len(roomIDs))
	for i, roomID := range roomIDs {
		n, err := cmds[i].Result()
		if err != nil {
			continue // 单个房间失败就跳过,不拖垮整批
		}
		out[roomID] = n
	}
	return out, nil

}