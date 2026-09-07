package ws

import (
	"sync"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"go.uber.org/zap"
)

// broadcastjob is the job for worker
type broadcastJob struct {
	roomId  int64
	msgType string
	payload []byte
}

// Broadcastpool is a shared worker pool for room broadcasts.
// Jobs for the same room are handled sequentially
// Jobs for different rooms are handled concurrently
type BroadcastPool struct {
	workers int

	queues []chan broadcastJob

	// deliver func support by Manager
	deliver func(roomId int64, msgType string, payload []byte)

	wg sync.WaitGroup
}

func NewBroadcastPool(workers int, deliver func(roomId int64, msgType string, payload []byte)) *BroadcastPool {
	if workers <= 0 {
		workers = 1
	}

	queues := make([]chan broadcastJob, workers)

	for i := range queues {
		queues[i] = make(chan broadcastJob, 1024)
	}

	return &BroadcastPool{
		workers: workers,
		deliver: deliver,
		queues:  queues,
	}
}

func (p *BroadcastPool) runWoker(index int) {
	defer p.wg.Done()

	for job := range p.queues[index] {
		if logger.DebugEnabled() {
			logger.L().Debug("broadcast job",
				zap.Int("worker", index),
				zap.Int64("room_id", job.roomId),
				zap.String("type", job.msgType),
			)
		}
		p.deliver(job.roomId, job.msgType, job.payload)
	}
}

func (p *BroadcastPool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)

		go p.runWoker(i)
	}
}

// room submit the broadcast jobs to the queue
// Returns false if the queue is full and the message is dropped.
func (p *BroadcastPool) Submit(roomId int64, msgType string, payload []byte) bool {
	idx := int(roomId % int64(p.workers))
	job := broadcastJob{
		roomId:  roomId,
		msgType: msgType,
		payload: payload,
	}

	select {
	case p.queues[idx] <- job:
		return true
	default:
		metrics.WSDropped.WithLabelValues("queue_full").Inc()

		if logger.DebugEnabled() {
			logger.L().Debug("broadcast queue full, drop message",
				zap.Int("queue", idx),
				zap.Int64("room_id", roomId),
				zap.String("type", msgType),
			)
		}
	}
	return false
}

func (p *BroadcastPool) Stop() {
	for _, q := range p.queues {
		close(q)
	}
	p.wg.Wait()
}
