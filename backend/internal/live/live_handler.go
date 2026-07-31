package live

import (
	"errors"
	"strconv"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/ginx"
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
	ownerId, roomId, ok := parseOwnerAndRoom(c)

	if !ok {
		return
	}

	SId, err := ls.LiveSerivce.LiveStart(c.Request.Context(), roomId, ownerId)

	if err != nil {
		WriteLiveError(c, err)
		return
	}
	response.Ok(c, gin.H{"session_id": SId})
}

// Stop handle POST /api/v1/rooms/:id/live/stop
func (ls *LiveHandler) LiveStop(c *gin.Context) {
	ownerId, roomId, ok := parseOwnerAndRoom(c)

	if !ok {
		return
	}

	err := ls.LiveSerivce.LiveStop(c, roomId, ownerId)

	if err != nil {
		WriteLiveError(c, err)
		return
	}

	response.Ok(c, gin.H{"room_id": roomId})
}

func parseOwnerAndRoom(c *gin.Context) (ownerId, roomId int64, ok bool) {
	ownerId, ok = ginx.GetContextValue[int64](c, "user_id")
	if !ok {
		response.Error(c, errno.InvalidRequest)
		return 0, 0, false
	}

	roomId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errno.InvalidRequest)
		return 0, 0, false
	}

	return ownerId, roomId, true
}

func WriteLiveError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repo.ErrRoomNotFound):
		response.Error(c, errno.RoomNotFound)
	case errors.Is(err, repo.ErrRoomForbidden):
		response.Error(c, errno.RoomForbidden)
	default:
		response.Error(c, errno.InternalError)
	}
}
