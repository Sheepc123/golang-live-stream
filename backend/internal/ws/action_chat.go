package ws

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"go.uber.org/zap"
)

const maxChatContentLength = 200

type ChatAction struct {
	manager    *Manager
	sessionMgr *live.SessionManager
}

func NewChatAction(m *Manager, sm *live.SessionManager) *ChatAction {
	return &ChatAction{manager: m, sessionMgr: sm}
}

func (a *ChatAction) Execute(c *Client, m Message) {
	content := strings.TrimSpace(m.Content)

	if content == "" {
		return
	}

	// Cut off the large lenth Danmu
	if utf8.RuneCountInString(content) > maxChatContentLength {
		runes := []rune(content)
		content = string(runes[:maxChatContentLength])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	sId, err := a.sessionMgr.ResolveID(ctx, c.RoomID)
	cancel()

	if err != nil {
		logger.L().Error("resolve session for chat fail",
			zap.Int64("room_id", c.RoomID),
			zap.Error(err),
		)
		return
	}

	if sId == 0 {
		// No session means this message could never be read back from history.

		logger.L().Debug("chat dropped, no active session",
			zap.Int64("room_id", c.RoomID),
			zap.Int64("user_id", c.UserID),
		)
		c.SendMsgOnlyOne(NewErrorMessage(c.RoomID, "the live is offline, cannot send danmu"))
		return
	}

	outMsg := NewChatMessage(c.UserID, c.RoomID, c.Username, content, sId)

	a.manager.BroadcastToRoom(c.RoomID, outMsg)

	a.manager.PersistMsg(outMsg)
}
