package handler

import (
	"strconv"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/gin-gonic/gin"
)

type MsgHandler struct {
	MsgService *service.MsgService
}

func NewMsgHandler(s *service.MsgService) *MsgHandler {
	return &MsgHandler{
		MsgService: s,
	}
}

// History handle the GET /api/v1/rooms/:id/msg?limit= ?
func (h *MsgHandler) History(c *gin.Context) {
	roomId, err := strconv.ParseInt(c.Param("id"), 10, 64)

	if err != nil {
		response.Fail(c, 400, errno.InvalidRequest.Code, errno.InvalidRequest.Msg, gin.H{})
		return
	}

	limit, _ := strconv.Atoi(c.Query("limit"))

	msgs, err := h.MsgService.GetHistoryMessage(c.Request.Context(), roomId, limit)

	if err != nil {
		response.Fail(c, 500, errno.InternalServerError.Code, errno.InternalServerError.Msg, gin.H{})
		return
	}

	items := make([]model.MsgResponse, 0, len(msgs))

	for i := range msgs {
		items = append(items, model.NewMsgResponse(&msgs[i]))
	}

	response.Ok(c, model.MessageListResponse{
		Messages: items,
		Total:    len(msgs),
	})

}
