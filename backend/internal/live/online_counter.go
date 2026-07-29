package live

/*
	onlineCounter tracks the number of viewers in a room using Redis
	and records the peak viewer count for each live session.
*/
import (
	"context"
	"fmt"
	"log"
	"time"

	"errors"

	"github.com/redis/go-redis/v9"
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
		log.Printf("online join fail (room=%d, user=%d): %v", roomId, userId, err)
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
		log.Printf("online leave fail (room=%d, user=%d): %v", roomId, userId, err)
		return 0
	}
	return n
}

func (c *OnlineCounter) Count(ctx context.Context, roomId int64) int64 {
	n, err := c.rdb.HLen(ctx, ViewersKey(roomId)).Result()

	if err != nil {
		log.Printf("online count fail (room=%d): %v", roomId, err)
		return 0
	}
	return n
}

var PeakScript = redis.NewScript(`
	local cur = tonumber(redis.call('GET',KEYS[1]) or '0')
	local new = tonumber(ARGV[1])

	if new > cur then 
		redis.call('SET',KEYS[1],new)
	end
	redis.call('EXPIRE',KEYS[1]),ARGV[2])
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
		log.Printf("update peak fail (session=%d): %v", sessionId, err)
	}
}

func (c *OnlineCounter) Peak(ctx context.Context, sessionId int64) int64 {
	n, err := c.rdb.Get(ctx, peakKey(sessionId)).Int64()

	if errors.Is(err, redis.Nil) {
		return 0
	}
	if err != nil {
		log.Printf("get peak fail (session=%d): %v", sessionId, err)
		return 0
	}
	return n
}
