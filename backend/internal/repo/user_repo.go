package repo

import (
	"context"
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"gorm.io/gorm"
)

var ErrUserNotFound = errors.New("user not found")

type UserRepo interface {
	FindByUsername(ctx context.Context, username string) (*entity.User, error)
}

type userRepo struct {
	db *gorm.DB
}

func NewUserRepo(db *gorm.DB) UserRepo {
	return &userRepo{db: db}
}

func (r *userRepo) FindByUsername(ctx context.Context, username string) (*entity.User, error) {
	var user entity.User

	err := r.db.WithContext(ctx).Where("username = ?", username).First(&user).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	return &user, nil
}
