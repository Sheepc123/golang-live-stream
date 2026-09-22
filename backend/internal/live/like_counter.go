package live

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const likeTTL = 24 * time.Hour

type LikeCounter struct {
	rdb *redis.Client
}

func NewLikeCounter(rdb *redis.Client) *LikeCounter {
	return &LikeCounter{rdb: rdb}
}

// generate redis key (e.g. live:sessionId:like)
// find by the live session Id
func LikeKey(SessionId int64) string {
	return fmt.Sprintf("live:session:%d:likes", SessionId)
}

// incrScript 把 INCR 和 EXPIRE 合成一次往返。
//
// ── 为什么值得为这个写 Lua ──
//
// 点赞是全项目最高频的写路径(计划里 5000 QPS,弹幕只有它的 1/2)。
// 原来的写法是两条独立命令:
//
//	c.rdb.Incr(...)    ← 一次 RTT
//	c.rdb.Expire(...)  ← 又一次 RTT
//
// 两次往返意味着两倍的网络延迟、两倍的连接池占用。
// 本机 Redis 的 RTT 大约 0.1~0.3ms,5000 QPS 下省掉的这一半
// 就是每秒 5000 次 syscall + 连接池里少一半的等待。
//
// 用 Lua 而不是 Pipeline,是因为 Lua 在 Redis 里是原子执行的:
// Pipeline 只是「打包发送」,两条命令之间仍然可能插进别的客户端的命令。
// 这里其实不需要原子性(INCR 和 EXPIRE 谁先谁后都无所谓),
// 但 Lua 还顺带省掉了一次 key 的字符串拼接 —— 原来 LikeKey()
// 被调用了两次,每次都是一次 fmt.Sprintf 的堆分配。
//
// ⚠️ 每次点赞都重置 TTL,所以这是一个「滑动 24 小时」而不是
// 「固定 24 小时」。对直播场次来说是对的:只要还有人在点赞,
// 这一场的计数就不该过期。
var incrScript = redis.NewScript(`
	local total = redis.call('INCR', KEYS[1])
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return total
`)

// Increase number of Likes
func (c *LikeCounter) Incr(ctx context.Context, SessionId int64) int64 {
	total, err := incrScript.Run(
		ctx,
		c.rdb,
		[]string{LikeKey(SessionId)},
		int64(likeTTL.Seconds()),
	).Int64()

	if err != nil {
		logger.L().Error("redis incr like fail",
			zap.Int64("session_id", SessionId), zap.Error(err),
		)
		return 0
	}

	return total
}

// GetHistoryLike returns the number of likes for the specified live Session
func (c *LikeCounter) GetHistoryLike(ctx context.Context, SessionId int64) (int64, error) {
	total, err := c.rdb.Get(ctx, LikeKey(SessionId)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}

	if err != nil {
		logger.L().Error("redis get like count fail",
			zap.Int64("session_id", SessionId), zap.Error(err),
		)
		return 0, err
	}
	return total, nil
}
