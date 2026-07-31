package ws

import (
	"context"
	"log"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/live"
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
		log.Printf("resolve session for chat fail (room=%d): %v", c.RoomID, err)
		return
	}

	if SId == 0 {
		log.Printf("like dropped, no active session (room=%d, user=%d)", c.RoomID, c.UserID)
		return
	}

	// Increment the number of like and return the total number
	total := a.likecounter.Incr(ctx, SId)
	LikeCountmsg := NewLikeMessageCount(c.RoomID, total)
	a.manager.BroadcastToRoom(c.RoomID, LikeCountmsg)

}
