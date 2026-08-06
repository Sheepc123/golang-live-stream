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

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var ev ChatEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			metrics.ConsumerMessages.WithLabelValues("unmarshal_error").Inc()
			logger.L().Error("consumer unmarshal fail",
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			session.MarkMessage(msg, "")
			continue
		}
		sendAt := ev.Timestamp
		if sendAt <= 0 {
			sendAt = time.Now().UnixMilli()
			logger.L().Warn("consumer missing timestamp, fallback to now",
				zap.Int64("room_id", ev.RoomID),
				zap.Int64("offset", msg.Offset),
			)
		}

		entityMsg := &entity.Message{
			EventID:       fmt.Sprintf("%d-%d", msg.Partition, msg.Offset),
			RoomID:        ev.RoomID,
			UserID:        ev.UserID,
			Username:      ev.Username,
			Content:       ev.Content,
			Type:          ev.Type,
			LiveSessionID: ev.LiveSessionID,
			SentAt:        sendAt,
		}
		start := time.Now()
		err := h.msgRepo.CreateIfAbsent(session.Context(), entityMsg)
		metrics.ConsumerWriteDuration.Observe(time.Since(start).Seconds())

		// Right now write msg to database one by one
		metrics.ConsumerBatchSize.Observe(1)

		if err != nil {
			metrics.ConsumerMessages.WithLabelValues("write_error").Inc()
			logger.L().Error("consumer write db fail",
				zap.Int64("room_id", ev.RoomID),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			return err

		}

		metrics.ConsumerLag.Observe(float64(time.Now().UnixMilli()-sendAt) / 1000)
		metrics.ConsumerMessages.WithLabelValues("ok").Inc()

		session.MarkMessage(msg, "")
	}
	return nil

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
