package entity

import "time"

type Message struct {
	ID       int64  `gorm:"primaryKey;autoIncrement"`
	RoomID   int64  `gorm:"index;not null"`
	UserID   int64  `gorm:"not null"`
	Username string `gorm:"size:64;not null"`
	Content  string `gorm:"size:500;not null"`
	Type     string `gorm:"size:16;not null"`

	LiveSessionID int64  `gorm:"not null;idx_session_sent,priority:1"`
	EventID       string `gorm:"size:64;uniqueIndex"`

	SentAt int64 `gorm:"not null;index:idx_session_sent,priority:2"`
	// autoCreateTime fill the time
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (Message) TableName() string {
	return "messages"
}
