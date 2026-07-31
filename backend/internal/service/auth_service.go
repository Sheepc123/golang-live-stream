package service

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"golang.org/x/crypto/bcrypt"
)

// 1. 逻辑业务判断实现 ：a.判断用户是否存在 b.账户密码正确性判断 简化 未使用数据库
// 2. 后端返回错误
// 3. 登录成功返回 profile
type AuthService struct {
	userRepo          repo.UserRepo
	jwtSecret         string
	accessTokenExpire time.Duration
}

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func NewAuthService(jwtcfg config.JWTConfig, userepo repo.UserRepo) *AuthService {
	return &AuthService{
		userRepo:          userepo,
		jwtSecret:         jwtcfg.Secret,
		accessTokenExpire: jwtcfg.AccessTokenExpire(),
	}
}

// Login service
func (s *AuthService) LoginService(ctx context.Context, login *model.LoginRequest) (*model.LoginResponse, error) {
	user, err := s.userRepo.FindByUsername(ctx, login.Username)

	if err != nil {
		if errors.Is(err, repo.ErrUserNotFound) {
			return nil, errno.InvalidCredentials
		}
		return nil, err
	}

	// verify password using bcrypt
	err = bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(login.Password))
	if err != nil {
		return nil, errno.InvalidCredentials
	}

	return s.issueToken(user)
}

func (s *AuthService) RegisterService(ctx context.Context, rg *model.RegisterRequest) (*model.LoginResponse, error) {
	username := strings.TrimSpace(rg.Username)

	if !usernamePattern.MatchString(username) {
		return nil, errno.InvalidRequest
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(rg.Password), bcrypt.DefaultCost)

	if err != nil {
		return nil, err
	}

	user := &entity.User{
		Username: username,
		Password: string(hashed),
	}

	if err := s.userRepo.Create(ctx, user); err != nil {
		return nil, err
	}
	return s.issueToken(user)

}

func (s *AuthService) issueToken(user *entity.User) (*model.LoginResponse, error) {
	accessToken, err := Jwttoken.GenerateAccessToken(
		user.ID,
		user.Username,
		s.jwtSecret,
		s.accessTokenExpire,
	)
	if err != nil {
		return nil, err
	}

	resp := &model.LoginResponse{
		AccessToken: accessToken,
		TokenType:   "Bearer",
		ExpiresIn:   int64(s.accessTokenExpire.Seconds()),
		User: model.UserProfile{
			Username: user.Username,
			UserID:   user.ID,
		},
	}

	return resp, nil
}
