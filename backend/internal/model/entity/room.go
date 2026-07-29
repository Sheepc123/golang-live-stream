package entity

import "time"

const (
	RoomStatusLive = "live"
	RoomStatusStop = "offline"
)

type Room struct {
	ID          int64  `gorm:"primaryKey;autoIncrement"`
	OwnerId     int64  `gorm:"index;not null"`
	Title       string `gorm:"size:128;not null"`
	ChannelName string `gorm:"size:64;not null"`
	Category    string `gorm:"size:32"`
	CoverURL    string `gorm:"size:512"`
	StreamURL   string `gorm:"size:512"`
	Description string `gorm:"size:512"`

	Status      string `gorm:"size:16;default:offline"`
	ViewerCount int64  `gorm:"default:0"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

// Return the table name
func (Room) TableName() string {
	return "rooms"
}
