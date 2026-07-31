package handler

import (
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/ginx"
	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/gin-gonic/gin"
)

type UserHandler struct{}

func NewUserHandler() *UserHandler {
	return &UserHandler{}
}

func (h *UserHandler) Me(c *gin.Context) {

	userProfile, ok := UserProfileFromContext(c)

	if !ok {
		response.Error(c, errno.Unauthorized)
		return
	}

	response.Ok(c, userProfile)
}

func UserProfileFromContext(c *gin.Context) (model.UserProfile, bool) {
	username, ok := ginx.GetContextValue[string](c, "user_name")

	if !ok {
		return model.UserProfile{}, false
	}

	userid, ok := ginx.GetContextValue[int64](c, "user_id")

	if !ok {
		return model.UserProfile{}, false
	}

	return model.UserProfile{
		Username: username,
		UserID:   userid,
	}, true
}
