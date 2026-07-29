package infra

import (
	"fmt"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

func AutoMigrate(db *gorm.DB) error {
	err := db.AutoMigrate(
		&entity.User{},
		&entity.Room{},
		&entity.LiveSession{},
		&entity.Message{},
	)

	if err != nil {
		return fmt.Errorf("failed to migrate : %w", err)
	}

	return nil
}

func Seed(db *gorm.DB) error {
	if err := seedUserS(db); err != nil {
		return err
	}

	if err := seedRooms(db); err != nil {
		return err
	}
	return nil
}

func seedUserS(db *gorm.DB) error {

	users := []entity.User{
		{
			Username: "admin0",
		},

		{
			Username: "admin1",
		},
		{
			Username: "admin2",
		},
	}

	for i := range users {
		hashed, err := bcrypt.GenerateFromPassword([]byte("123456"), bcrypt.DefaultCost)

		if err != nil {
			return fmt.Errorf("failed to hash password %w", err)
		}

		user := users[i]
		user.Password = string(hashed)

		result := db.Where(entity.User{Username: user.Username}).Attrs(entity.User{Password: user.Password}).FirstOrCreate(&user)

		if result.Error != nil {
			return fmt.Errorf("seed user %s : %w", user.Username, result.Error)
		}
	}
	return nil

}

func seedRooms(db *gorm.DB) error {

	var owner entity.User
	if err := db.Where("username = ?", "admin0").First(&owner).Error; err != nil {
		return fmt.Errorf("seed rooms: find owner admin0: %w", err)
	}

	rooms := []entity.Room{
		{
			OwnerId:     owner.ID,
			Title:       "Music Live",
			ChannelName: "Time Music",
			Category:    "Music",
			CoverURL:    "https://picsum.photos/seed/music-live/640/360",
			StreamURL:   "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
			Description: "A relaxing music live channel.",
			Status:      "live",
			ViewerCount: 1280,
		},
		{
			OwnerId:     owner.ID,
			Title:       "Game Arena",
			ChannelName: "Time Gaming",
			Category:    "Gaming",
			CoverURL:    "https://picsum.photos/seed/game-arena/640/360",
			StreamURL:   "https://test-streams.mux.dev/test_001/stream.m3u8",
			Description: "Live gameplay and tournament highlights.",
			Status:      "live",
			ViewerCount: 3421,
		},
		{
			OwnerId:     owner.ID,
			Title:       "Tech Talk",
			ChannelName: "Time Tech",
			Category:    "Technology",
			CoverURL:    "https://picsum.photos/seed/tech-talk/640/360",
			StreamURL:   "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
			Description: "Tech news, demos, and product talks.",
			Status:      "live",
			ViewerCount: 876,
		},
	}
	for i := range rooms {
		r := rooms[i]
		// 按 Title 判断是否已存在，避免重复灌。
		result := db.Where(entity.Room{Title: r.Title}).FirstOrCreate(&rooms[i])
		if result.Error != nil {
			return fmt.Errorf("seed room %s: %w", r.Title, result.Error)
		}
	}
	return nil
}
