package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net"
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
	drivermysql "github.com/go-sql-driver/mysql"
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

	// Include the initial write: at most two retries after the first attempt.
	consumerWriteMaxAttempts = 3

	// Wait 200ms before the second attempt and 400ms before the third.
	consumerRetryBaseDelay = 200 * time.Millisecond
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

// Retry recognized connection, timeout, and lock errors. Unknown errors and
// invalid data fail immediately; retrying cannot repair a malformed record.
func isRetryableWriteError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, drivermysql.ErrInvalidConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var mysqlErr *drivermysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1205, 1213:
			// Lock wait timeout or deadlock: retry the repository transaction.
			return true
		default:
			return false
		}
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

// Keep the same batch until a write succeeds or retries are exhausted.
// Kafka progress remains the caller's responsibility after confirmed success.
func (h *consumerHandler) writeBatchWithRetry(ctx context.Context, batch []entity.Message) error {
	if len(batch) == 0 {
		return nil
	}

	var lastErr error
	delay := consumerRetryBaseDelay
	for attempt := 1; attempt <= consumerWriteMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Never reuse an expired attempt context. Every write still belongs
		// to the session, so cancellation interrupts in-flight database work.
		writeCtx, cancel := context.WithTimeout(ctx, consumerWriteTimeout)
		start := time.Now()
		lastErr = h.msgRepo.CreateBatchIfAbsent(writeCtx, batch)
		cancel()
		// Observe individual database attempts, excluding backoff time.
		metrics.ConsumerWriteDuration.Observe(time.Since(start).Seconds())
		if lastErr == nil {
			return nil
		}

		// Session cancellation takes precedence over retryable write errors.
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isRetryableWriteError(lastErr) || attempt == consumerWriteMaxAttempts {
			return lastErr
		}

		logger.L().Warn("consumer batch write retry scheduled",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", consumerWriteMaxAttempts),
			zap.Int("batch_size", len(batch)),
			zap.Duration("retry_delay", delay),
			zap.Error(lastErr),
		)

		// A cancellable timer allows the claim to exit during a rebalance.
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			timer.Stop()
		}
		delay *= 2
	}
	return lastErr
}

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

		err := h.writeBatchWithRetry(ctx, batch)

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
				EventID:       fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset),
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

	// 端口从配置来,不再硬编码。consumer 和 server 是两个进程,
	// 容器化之后它们各自的 metrics 端口必须能独立注入
	// (prometheus.yml 里要按服务名 + 端口分别配 scrape target)。
	metricsAddr := ":" + cfg.Server.MetricsPort
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{
			Addr:    metricsAddr,
			Handler: mux,
			// 不用 http.ListenAndServe 的裸默认值:没有超时的 Server
			// 会被慢速请求一直占着连接。这个端口只给 Prometheus 抓,
			// 但它同样暴露在内网里。
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.L().Error("consumer metrics server error", zap.Error(err))
		}
	}()
	logger.L().Info("consumer metrics listening", zap.String("addr", metricsAddr))

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
