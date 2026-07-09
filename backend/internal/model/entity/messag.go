package entity

import "time"

type Message struct {
	ID int64 `gorm:"primaryKey;autoIncrement"`
	
	RoomID int64 `gorm:"index;not null"`

	UserID   int64  `gorm:"not null"`
	Username string `gorm:"size:64;not null"`

	Content string `gorm:"size:500;not null"`

	Type string `gorm:"size:16;not null"`

	// autoCreateTime fill the time
	CreatedAt time.Time `gorm:"autoCreateTime;index"`
}

func (Message) TableName() string {
	return "messages"
}