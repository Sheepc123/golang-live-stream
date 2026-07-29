package live

import (
	"errors"
	"strconv"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/handler"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/gin-gonic/gin"
)

type LiveHandler struct {
	LiveSerivce *LiveService
}

func NewLiveHandler(ls *LiveService) *LiveHandler {
	return &LiveHandler{LiveSerivce: ls}
}

// Start handle POST /api/v1/rooms/:id/live/start —— 主播点“开播”
func (ls *LiveHandler) LiveStart(c *gin.Context) {
	ownerId, ok := handler.GetContextValue[int64](c, "user_id")
	if !ok {
		response.Fail(c, 401, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	roomId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	SId, err := ls.LiveSerivce.LiveStart(c, roomId, ownerId)

	if err != nil {
		switch {
		case errors.Is(err, repo.ErrRoomNotFound):
			response.Fail(c, 404, errno.RoomNotFound.Code, errno.RoomNotFound.Msg, gin.H{})
		case errors.Is(err, repo.ErrRoomForbidden):
			response.Fail(c, 403, errno.Forbidden.Code, errno.Forbidden.Msg, gin.H{})
		default:
			response.Fail(c, 500, errno.InternalServerError.Code, errno.InternalServerError.Msg, gin.H{})
		}
		return
	}
	response.Ok(c, gin.H{"session_id": SId})
}

// Stop handle POST /api/v1/rooms/:id/live/stop —— 主播点“下播”
func (ls *LiveHandler) LiveStop(c *gin.Context) {
	ownerId, ok := handler.GetContextValue[int64](c, "user_id")
	if !ok {
		response.Fail(c, 401, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	roomId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	err = ls.LiveSerivce.LiveStop(c, roomId, ownerId)

	if err != nil {
		switch {
		case errors.Is(err, repo.ErrRoomNotFound):
			response.Fail(c, 404, errno.RoomNotFound.Code, errno.RoomNotFound.Msg, gin.H{})
		case errors.Is(err, repo.ErrRoomForbidden):
			response.Fail(c, 403, errno.Forbidden.Code, errno.Forbidden.Msg, gin.H{})
		default:
			response.Fail(c, 500, errno.InternalServerError.Code, errno.InternalServerError.Msg, gin.H{})
		}
		return
	}

	response.Ok(c, gin.H{"room_id": roomId})
}
