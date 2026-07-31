package handler

import (
	"context"
	"strconv"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/model"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/gin-gonic/gin"
)

type MsgHandler struct {
	MsgService *service.MsgService
	SMgr       *live.SessionManager
}

func NewMsgHandler(s *service.MsgService, ls *live.SessionManager) *MsgHandler {
	return &MsgHandler{
		MsgService: s,
		SMgr:       ls,
	}
}

// History handle the GET /api/v1/rooms/:id/msg?limit= ?
func (h *MsgHandler) History(c *gin.Context) {
	roomId, err := strconv.ParseInt(c.Param("id"), 10, 64)

	if err != nil {
		response.Error(c, errno.InvalidRequest)
		return
	}

	limit, _ := strconv.Atoi(c.Query("limit"))

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	sessionId, err := h.SMgr.ResolveID(ctx, roomId)

	if err != nil {
		response.Error(c, err)
		return
	}

	// when sessionId = 0, it means there is no current session, so no history.
	if sessionId == 0 {
		response.Ok(c, model.MessageListResponse{
			Messages: []model.MsgResponse{},
			Total:    0,
		})

		return
	}

	msgs, err := h.MsgService.GetHistoryMessage(ctx, roomId, sessionId, limit)

	if err != nil {
		response.Error(c, err)
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
