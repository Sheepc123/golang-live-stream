package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/router"
)

func main() {

	// Config Load
	// Config setting includes config.yaml and .env
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config : %v", err)
	}

	// Mysql load
	db, err := infra.NewMySQL(cfg.MySQL)

	if err != nil {
		log.Fatalf("failed to connect mysql : %v", err)
	}

	log.Printf("mysql connected")

	// AutoMigrate databse
	if err := infra.AutoMigrate(db); err != nil {
		log.Fatalf("failed to migrate %v", err)
	}

	log.Printf("successfully migrate database")

	// Seed generate inital database
	if err := infra.Seed(db); err != nil {
		log.Fatalf("failed to seed database : %v", err)
	}
	log.Printf("successfully seed database")

	// NewRouter
	r, wsManager := router.NewRouter(cfg, db)

	srv := &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: r,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	defer stop() // 退出时解绑信号，恢复默认行为
	go func() {
		log.Printf("server listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("failed to start server : %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutdown signal received, closing gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	wsManager.ShutDown()

	log.Printf("server exited cleanly")
}
