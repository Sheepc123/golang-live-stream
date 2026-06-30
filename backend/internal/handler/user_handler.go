package handler

import (
	"github.com/Sheepc123/golang-live-stream/internal/errno"
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
		response.Fail(c, 401, errno.Unauthorized.Code, errno.Unauthorized.Msg, gin.H{})
	}

	response.Ok(c, userProfile)
}

func UserProfileFromContext(c *gin.Context) (model.UserProfile, bool) {
	username, ok := getContextValue[string](c, "user_name")

	if !ok {
		return model.UserProfile{}, false
	}

	useid, ok := getContextValue[int64](c, "user_id")

	if !ok {
		return model.UserProfile{}, false
	}

	return model.UserProfile{
		Username: username,
		UserID:   useid,
	}, true
}

func getContextValue[T any](c *gin.Context, key string) (T, bool) {
	value, ok := c.Get(key)

	if !ok {
		var zero T
		return zero, false
	}

	realValue, ok := value.(T)

	if !ok {
		var zero T
		return zero, false
	}

	return realValue, true
}
