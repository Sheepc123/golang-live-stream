package main

import (
	"context"
	"errors"
	stdlog "log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/router"
	"go.uber.org/zap"
)

func main() {

	// Config Load
	// Config setting includes config.yaml and .env
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		stdlog.Fatalf("failed to load config : %v", err)
	}

	if err := logger.Init(cfg.Log); err != nil {
		stdlog.Fatalf("failed to init logger: %v", err)
	}
	defer logger.Sync()

	logger.L().Info("logger initialized",
		zap.String("level", cfg.Log.Level),
		zap.String("format", cfg.Log.Format),
	)

	// Mysql load
	db, err := infra.NewMySQL(cfg.MySQL)

	if err != nil {
		logger.L().Fatal("failed to connect mysql", zap.Error(err))
	}

	logger.L().Info("mysql connected")

	// AutoMigrate databse
	if err := infra.AutoMigrate(db); err != nil {
		logger.L().Fatal("failed to migrate database", zap.Error(err))
	}

	logger.L().Info("database migrated")

	// Seed generate inital database
	if err := infra.Seed(db); err != nil {
		logger.L().Fatal("failed to seed database", zap.Error(err))
	}
	logger.L().Info("database seeded")

	// Redis Load
	rdb, err := infra.NewRedis(cfg.Redis)
	if err != nil {
		logger.L().Fatal("failed to connect redis", zap.Error(err))
	}

	logger.L().Info("redis connected")

	// New Kafka
	producer, err := infra.NewKafkaProducer(cfg.Kafka)

	if err != nil {
		logger.L().Fatal("failed to create kafka producer", zap.Error(err))
	}
	logger.L().Info("kafka producer ready")

	// NewRouter
	r, wsManager := router.NewRouter(cfg, db, rdb, producer)

	srv := &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: r,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	defer stop() // 退出时解绑信号，恢复默认行为
	go func() {
		logger.L().Info("server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.L().Fatal("failed to start server", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.L().Info("shutdown signal received, closing gracefully")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.L().Error("http server shutdown error", zap.Error(err))
	}

	wsManager.ShutDown()

	if err := producer.Close(); err != nil {
		logger.L().Error("kafka producer close error", zap.Error(err))
	}

	logger.L().Info("server exited cleanly")

}
