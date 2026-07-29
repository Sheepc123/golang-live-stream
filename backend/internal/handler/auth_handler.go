package handler

import (
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

// NewAuthHandler creates an AuthcHandler with its required dependencies.
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
		response.Error(c, errno.InvalidRequest)
		return
	}

	loginResp, err := h.authService.LoginService(c.Request.Context(), &req)

	if err != nil {
		response.Error(c, err)
	}
	response.Ok(c, loginResp)
}
