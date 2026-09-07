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

// Increase number of Likes
func (c *LikeCounter) Incr(ctx context.Context, SessionId int64) int64 {
	total, err := c.rdb.Incr(ctx, LikeKey(SessionId)).Result()

	if err != nil {
		logger.L().Error("redis incr like fail",
			zap.Int64("session_id", SessionId), zap.Error(err),
		)
		return 0
	}

	c.rdb.Expire(ctx, LikeKey(SessionId), likeTTL)
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
