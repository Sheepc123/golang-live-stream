package handler

import (
	"strconv"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/ginx"
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
	rooms, err := h.roomService.RoomList(c.Request.Context())

	if err != nil {
		response.Error(c, err)
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
		response.Error(c, errno.InvalidRequest)
		return
	}

	room, err := h.roomService.GetRoomByID(c.Request.Context(), id)

	if err != nil {
		response.Error(c, err)
		return
	}

	response.Ok(c, model.NewRoomResponse(room))
}

// GETMineRoom handler GET /api/v1/rooms/mine
func (h *RoomHandler) ListMyRoom(c *gin.Context) {

	ownerId, exits := ginx.GetContextValue[int64](c, "user_id")

	if !exits {
		response.Error(c, errno.Unauthorized)
		return
	}

	rooms, err := h.roomService.ListMyRoom(c.Request.Context(), ownerId)

	if err != nil {
		response.Error(c, err)
	}

	roomsRepose := make([]model.RoomResponse, 0, len(rooms))

	for i := range rooms {
		roomsRepose = append(roomsRepose, model.NewRoomResponse(&rooms[i]))
	}
	response.Ok(c, model.RoomListResponse{
		Rooms: roomsRepose,
		Total: len(roomsRepose),
	})
}

// createRoom handle POST /api/v1/rooms
func (h *RoomHandler) CreatRoom(c *gin.Context) {
	ownerId, ok := ginx.GetContextValue[int64](c, "user_id")

	if !ok {
		response.Error(c, errno.Unauthorized)
		return
	}

	var req model.CreateRoomRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errno.InvalidRequest)
		return
	}

	room, err := h.roomService.Create(c.Request.Context(), ownerId, &req)

	if err != nil {
		response.Error(c, err)
		return
	}

	response.Ok(c, model.NewRoomResponse(room))
}

// UpdateRoom handle PUT /api/v1/rooms/id
func (h *RoomHandler) UpdateRoom(c *gin.Context) {
	ownerId, ok := ginx.GetContextValue[int64](c, "user_id")

	if !ok {
		response.Error(c, errno.InvalidRequest)
		return
	}

	RoomId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errno.InvalidRequest)
		return
	}

	var req model.UpdateRoomRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, err)
		return
	}

	room, err := h.roomService.UpdateRoom(c.Request.Context(), ownerId, RoomId, &req)

	if err != nil {
		// a. Room not found   404
		// b. This user do not have the authority ,Forbidden 403
		// c. others

		response.Error(c, err)
		return

	}
	response.Ok(c, model.NewRoomResponse(room))

}

// DeleteRoom handle DELETE /api/v1/rooms/:id
func (h *RoomHandler) DeleteRoom(c *gin.Context) {
	ownerId, ok := ginx.GetContextValue[int64](c, "user_id")

	if !ok {
		response.Error(c, errno.Unauthorized)
		return
	}

	RoomId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, errno.InvalidRequest)
		return
	}

	err = h.roomService.DeleteRoom(c.Request.Context(), ownerId, RoomId)

	if err != nil {
		response.Error(c, err)
		return

	}

	response.Ok(c, gin.H{"room_id": RoomId})
}
