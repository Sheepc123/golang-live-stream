package ws

import (
	"context"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"go.uber.org/zap"
)

type LikeAction struct {
	manager     *Manager
	likecounter *live.LikeCounter
	sessionMgr  *live.SessionManager
}

func NewLikeAction(m *Manager, lc *live.LikeCounter, sm *live.SessionManager) *LikeAction {
	return &LikeAction{
		manager:     m,
		likecounter: lc,
		sessionMgr:  sm,
	}
}

func (a *LikeAction) Execute(c *Client, m Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	SId, err := a.sessionMgr.ResolveID(ctx, c.RoomID)

	if err != nil {
		logger.L().Error("resolve session for like fail",
			zap.Int64("room_id", c.RoomID),
			zap.Error(err),
		)
		return
	}

	if SId == 0 {
		logger.L().Debug("like dropped, no active session",
			zap.Int64("room_id", c.RoomID),
			zap.Int64("user_id", c.UserID),
		)
		return
	}

	// Increment the number of like and return the total number
	total := a.likecounter.Incr(ctx, SId)
	LikeCountmsg := NewLikeMessageCount(c.RoomID, total)
	a.manager.BroadcastToRoom(c.RoomID, LikeCountmsg)

}
