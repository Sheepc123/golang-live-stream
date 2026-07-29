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
		response.Error(c, errno.Unauthorized)
		return
	}

	response.Ok(c, userProfile)
}

func UserProfileFromContext(c *gin.Context) (model.UserProfile, bool) {
	username, ok := GetContextValue[string](c, "user_name")

	if !ok {
		return model.UserProfile{}, false
	}

	userid, ok := GetContextValue[int64](c, "user_id")

	if !ok {
		return model.UserProfile{}, false
	}

	return model.UserProfile{
		Username: username,
		UserID:   userid,
	}, true
}

// get the T from context
// from JWT Token we can get the user_id and User_name
func GetContextValue[T any](c *gin.Context, key string) (T, bool) {
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
