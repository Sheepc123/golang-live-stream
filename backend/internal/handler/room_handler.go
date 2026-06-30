package handler

import (
	"errors"
	"strconv"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/gin-gonic/gin"
)

type RoomHandler struct {
	roomService *service.RoomService
}

func NewRoomHandler(r *service.RoomService) *RoomHandler {
	return &RoomHandler{
		roomService: r,
	}
}

// List Room handles GET/api/v1/rooms
func (h *RoomHandler) ListRoom(c *gin.Context) {
	rooms, err := h.roomService.RoomList()

	if err != nil {
		response.Fail(c, 404, errno.InternalServerError.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	roomResponse := make([]model.RoomResponse, 0, len(rooms))

	for i := range rooms {
		roomResponse = append(roomResponse, model.NewRoomResponse(&rooms[i]))
	}

	response.Ok(c, model.RoomListResponse{
		Rooms: roomResponse,
		Total: len(roomResponse),
	})
}

// GetRoomBy handle GET /api/v1/rooms/:id  - return single live-stream
func (h *RoomHandler) GetRoomByID(c *gin.Context) {
	idstr := c.Param("id")

	id, err := strconv.ParseInt(idstr, 10, 64)

	if err != nil {
		response.Fail(c, 400, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	room, err := h.roomService.GetRoomByID(id)

	if err != nil {
		switch {
		case errors.Is(err, service.ErrRoomNotFound):
			response.Fail(c, 404, errno.RoomNotFound.Code, errno.RoomNotFound.Msg, gin.H{})
		default:
			response.Fail(c, 500, errno.InternalServerError.Code, errno.InternalServerError.Msg, gin.H{})
		}
		return
	}

	response.Ok(c, room)
}
