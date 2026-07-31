package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
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
			log.Printf("consumer unmarshal fail (offset=%d): %v", msg.Offset, err)
			session.MarkMessage(msg, "")
			continue
		}
		sendAt := ev.Timestamp
		if sendAt <= 0 {
			sendAt = time.Now().UnixMilli()
			log.Printf("consumer: missing timestamp, fallback to now (room=%d, offset=%d)",
				ev.RoomID, msg.Offset)
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

		if err := h.msgRepo.CreateIfAbsent(session.Context(), entityMsg); err != nil {
			log.Printf("consumer write db fail (room=%d, offset=%d): %v", ev.RoomID, msg.Offset, err)
			return err
		}
		session.MarkMessage(msg, "")
	}
	return nil

}

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config : %v", err)
	}

	db, err := infra.NewMySQL(cfg.MySQL)

	if err != nil {
		log.Fatalf("consumer failed to load Database : %v", err)
	}

	log.Printf("consumer: mysql connected")

	msgRepo := repo.NewMesRep(db)

	sc := sarama.NewConfig()
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.GroupId, sc)

	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}
	defer group.Close()

	handler := &consumerHandler{msgRepo: msgRepo}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		if err := group.Consume(ctx, []string{cfg.Kafka.Topic}, handler); err != nil {
			log.Printf("consumer error")
		}
		if ctx.Err() != nil {
			log.Printf("consumer shutdown signal received, exiting")
			return
		}
	}

}
