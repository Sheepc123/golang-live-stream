package main

import (
	"log"

	"github.com/Sheepc123/golang-live-stream/internal/router"
)

func main() {

	r := router.NewRouter()

	err := r.Run(":8080")

	if err != nil {
		log.Fatalf("failed to start server : %v", err)
	}
}
