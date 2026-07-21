type LiveSession struct {
	ID              int64      `gorm:"primaryKey;autoIncrement"`
	RoomID          int64      `gorm:"not null;index"`
	StartedAt       time.Time  `gorm:"not null;index"`
	EndedAt         *time.Time `gorm:"index"`
	LikeCount       int64      `gorm:"not null;default:0"`
	PeakOnlineCount int64      `gorm:"not null;default:0"`

	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (LiveSession) TableName() string {
	return "live_sessions1"
}