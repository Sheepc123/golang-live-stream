package ws

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type LikeCounter struct {
	rdb *redis.Client
}

func NewLikeCounter(rdb *redis.Client) *LikeCounter {
	return &LikeCounter{
		rdb: rdb,
	}
}

// generate redis key (e.g. room:1:like)
func LikeRoomKey(roomId int64) string {
	return fmt.Sprintf("room:%d:like", roomId)
}

func (c *LikeCounter) Incr(roomId int64) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	total, err := c.rdb.Incr(ctx, LikeRoomKey(roomId)).Result()

	if err != nil {
		log.Printf("redis failed to incr (roomId = %d): %v", roomId, err)
		return 0
	}

	return total
}

func (c *LikeCounter) GetHistoryLike(roomId int64) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	total, err := c.rdb.Get(ctx, LikeRoomKey(roomId)).Int64()

	if err == redis.Nil {
		return 0
	}

	if err != nil {
		log.Printf("redis failed to get like (roomId = %d) : %v", roomId, err)
		return 0
	}
	return total
}

func (c *LikeCounter) ResetLike(ctx context.Context, roomId int64) error {
	return c.rdb.Del(ctx, LikeRoomKey(roomId)).Err()
}
