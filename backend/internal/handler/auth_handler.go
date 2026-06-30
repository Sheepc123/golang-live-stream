package handler

import (
	"errors"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/gin-gonic/gin"
)

// 1.handler实现 接受前端发来的信息 把JSON解析成model.LoginRequest

// Handler represents the hanlder
type AuthHandler struct {
	authService *service.AuthService
}

// NewAuthHandler creates an AuthHandler with its required dependencies.
func NewAuthHandler(authService *service.AuthService) *AuthHandler {
	return &AuthHandler{
		authService: authService,
	}
}

// Login handles user login requires.
// /api/v1/login
func (h *AuthHandler) Login(c *gin.Context) {

	var req model.LoginRequest
	err := c.ShouldBindJSON(&req)
	if err != nil {
		response.Fail(c, 400, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	loginResp, err := h.authService.LoginService(&req)

	if err != nil {
		switch {
		case errors.Is(err, service.ErrUserNotFound):
			response.Fail(c, 400, errno.UserNotFound.Code, errno.UserNotFound.Msg, gin.H{})

		case errors.Is(err, service.ErrPasswordWrong):
			response.Fail(c, 400, errno.WrongPassword.Code, errno.WrongPassword.Msg, gin.H{})

		default:
			response.Fail(c, 500, errno.InternalServerError.Code, errno.InternalServerError.Msg, gin.H{})
		}
		return
	}
	response.Ok(c, loginResp)
}
