package main

import (
	"log"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/router"
)

func main() {

	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config : %v", err)
	}

	db, err := infra.NewMySQL(cfg.MySQL)

	if err != nil {
		log.Fatalf("failed to connect mysql : %v", err)
	}

	log.Printf("mysql connected")

	if err := infra.AutoMigrate(db); err != nil {
		log.Fatalf("failed to migrate %v", err)
	}
	log.Printf("successfully migrate database")

	// if err := infra.Seed(db); err != nil {
	// 	log.Fatalf("failed to seed database : %v", err)
	// }
	// log.Printf("successfully seed database")

	r := router.NewRouter(cfg, db)

	if err := r.Run(":" + cfg.Server.Port); err != nil {
		log.Fatalf("failed to start server : %v", err)
	}
}
