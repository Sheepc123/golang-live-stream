package ws

import "sync"

type LikeCounter struct {
	counts map[int64]int64

	mu sync.Mutex
}

func NewLikeCounter() *LikeCounter {
	return &LikeCounter{
		counts: make(map[int64]int64),
	}
}

func (c *LikeCounter) Incr(roomId int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counts[roomId]++
	return c.counts[roomId]
}

func (c *LikeCounter) GetHistoryLike(roomId int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[roomId]
}
