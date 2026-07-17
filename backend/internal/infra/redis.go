package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/redis/go-redis/v9"
)

func NewRedis(cfg config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis : %w", err)
	}
	return rdb, nil
}
