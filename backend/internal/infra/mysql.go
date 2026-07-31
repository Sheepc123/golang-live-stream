package infra

import (
	"fmt"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewMySQL(cfg config.MySQLConfig) (*gorm.DB, error) {
	dsn := cfg.DSN()

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:         logger.Default.LogMode(logger.Info),
		TranslateError: true,
	})

	if err != nil {
		return nil, fmt.Errorf("open mysql : %v", err)

	}

	sqlDB, err := db.DB()

	if err != nil {
		return nil, fmt.Errorf("get sql.DB : %w", err)
	}

	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MAXIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetime) * time.Second)

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping mysql : %w", err)
	}

	return db, nil
}
