package main

import (
	"context"
	"encoding/json"
	"fmt"
	stdlog "log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	// Flush when this partition has accumulated enough messages.
	consumerBatchSize = 500

	// Periodically flush smaller batches during low traffic.
	consumerFlushInterval = 100 * time.Millisecond

	// Bound each database write attempt.
	consumerWriteTimeout = 3 * time.Second
)

// ChatEvent Read from Kafka struct
type ChatEvent struct {
	Type          string `json:"type"`
	RoomID        int64  `json:"room_id"`
	UserID        int64  `json:"user_id"`
	Username      string `json:"username"`
	Content       string `json:"content"`
	Timestamp     int64  `json:"timestamp"`
	LiveSessionID int64  `json:"live_session_id"`
}

// consumerHandler implements the interface of sarma.ConsumerGroupHandler
type consumerHandler struct {
	msgRepo repo.MsgRepo
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }


func (h *consumerHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	ctx := session.Context()

	// Each invocation belongs to one partition.
	// Keep its batch local instead of sharing it through consumerHandler.
	batch := make([]entity.Message, 0, consumerBatchSize)

	// Track the last Kafka message represented by the current batch.
	var lastMessage *sarama.ConsumerMessage

	ticker := time.NewTicker(consumerFlushInterval)
	defer ticker.Stop()

	// This closure reads and updates the current invocation's batch.
	// It runs synchronously in the consume loop.
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		writeCtx, cancel := context.WithTimeout(
			ctx,
			consumerWriteTimeout,
		)

		start := time.Now()
		err := h.msgRepo.CreateBatchIfAbsent(writeCtx, batch)
		cancel()

		metrics.ConsumerWriteDuration.Observe(
			time.Since(start).Seconds(),
		)

		if err != nil {
			metrics.ConsumerMessages.
				WithLabelValues("write_error").
				Add(float64(len(batch)))

			logger.L().Error("consumer batch write failed",
				zap.String("topic", claim.Topic()),
				zap.Int32("partition", claim.Partition()),
				zap.Int("batch_size", len(batch)),
				zap.Error(err),
			)

			// Leave the batch unmarked so it can be replayed.
			return err
		}

		// All earlier messages in this partition's batch are now durable.
		// Marking is not the same as immediately committing to Kafka.
		session.MarkMessage(lastMessage, "")

		metrics.ConsumerBatchSize.Observe(float64(len(batch)))
		metrics.ConsumerMessages.
			WithLabelValues("ok").
			Add(float64(len(batch)))

		now := time.Now().UnixMilli()
		for _, msg := range batch {
			metrics.ConsumerLag.Observe(
				float64(now-msg.SentAt) / 1000,
			)
		}

		// Release references to message strings, then reuse the allocation.
		clear(batch)
		batch = batch[:0]
		lastMessage = nil

		return nil
	}

	for {
		// Prefer exiting once this session has been cancelled.
		if ctx.Err() != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			// Do not start a new write with a cancelled session.
			// Unmarked messages remain eligible for replay.
			return nil

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		case msg, ok := <-claim.Messages():
			if !ok {
				if ctx.Err() != nil {
					return nil
				}

				// Flush the final partial batch if the session is still active.
				return flush()
			}

			var ev ChatEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				metrics.ConsumerMessages.
					WithLabelValues("unmarshal_error").
					Inc()

				logger.L().Error("consumer unmarshal failed",
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.Error(err),
				)

				// Persist earlier valid messages before advancing past
				// this malformed message.
				if err := flush(); err != nil {
					return err
				}

				// Preserve the existing policy: log and skip malformed JSON.
				session.MarkMessage(msg, "")
				continue
			}

			sentAt := ev.Timestamp
			if sentAt <= 0 {
				sentAt = time.Now().UnixMilli()

				logger.L().Warn("consumer missing timestamp, fallback to now",
					zap.Int64("room_id", ev.RoomID),
					zap.Int64("offset", msg.Offset),
				)
			}

			batch = append(batch, entity.Message{
				EventID:       fmt.Sprintf("%d-%d", msg.Partition, msg.Offset),
				RoomID:        ev.RoomID,
				UserID:        ev.UserID,
				Username:      ev.Username,
				Content:       ev.Content,
				Type:          ev.Type,
				LiveSessionID: ev.LiveSessionID,
				SentAt:        sentAt,
			})

			lastMessage = msg

			if len(batch) >= consumerBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		stdlog.Fatalf("failed to load config : %v", err)
	}

	if err := logger.Init(cfg.Log); err != nil {
		stdlog.Fatalf("failed to init logger; %v", err)
	}
	defer logger.Sync()

	db, err := infra.NewMySQL(cfg.MySQL)

	if err != nil {
		logger.L().Fatal("consumer failed to connect mysql", zap.Error(err))
	}

	logger.L().Info("consumer: mysql connected")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9101", mux); err != nil {
			logger.L().Error("consumer metrics server error", zap.Error(err))
		}
	}()
	logger.L().Info("consumer metrics listening", zap.String("addr", ":9101"))

	msgRepo := repo.NewMesRep(db)

	sc := sarama.NewConfig()
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.GroupId, sc)

	if err != nil {
		logger.L().Fatal("failed to create consumer group", zap.Error(err))
	}
	defer group.Close()

	handler := &consumerHandler{msgRepo: msgRepo}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.L().Info("consumer started",
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("topic", cfg.Kafka.Topic),
		zap.String("group", cfg.Kafka.GroupId),
	)

	for {
		if err := group.Consume(ctx, []string{cfg.Kafka.Topic}, handler); err != nil {
			logger.L().Error("consumer group error", zap.Error(err))
		}
		if ctx.Err() != nil {
			logger.L().Info("consumer shutdown signal received, exiting")
			return
		}
	}

}
