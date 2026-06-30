package service

import (
	"errors"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/model"
	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
)

// 1. 逻辑业务判断实现 ：a.判断用户是否存在 b.账户密码正确性判断 简化 未使用数据库
// 2. 后端返回错误
// 3. 登录成功返回 profile
type AuthService struct {
	testUser model.User
}

const (
	jwtSecret        = "dev-secret"
	acessToeknExpire = 2 * time.Hour
)

var (
	ErrUserNotFound  = errors.New("User can not Found")
	ErrPasswordWrong = errors.New("PassWord Wrong")
)

func NewAuthService() *AuthService {
	return &AuthService{
		testUser: model.User{
			Username: "admin",
			Password: "123456",
			UserID:   1,
		},
	}
}

func (s *AuthService) LoginService(login *model.LoginRequest) (*model.LoginResponse, error) {
	if login.Username != s.testUser.Username {
		return nil, ErrUserNotFound
	}

	if login.Password != s.testUser.Password {
		return nil, ErrPasswordWrong
	}

	accessToken, err := Jwttoken.GenerateAccessToken(
		s.testUser.UserID,
		s.testUser.Username,
		jwtSecret,
		acessToeknExpire,
	)
	if err != nil {
		return nil, err
	}

	resp := &model.LoginResponse{
		AccessToken: accessToken,
		TokenType:   "Bearer",
		ExpiresIn:   int64(acessToeknExpire.Seconds()),
		User: model.UserProfile{
			Username: s.testUser.Username,
			UserID:   s.testUser.UserID,
		},
	}

	return resp, nil
}
