package ws

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Sheepc123/golang-live-stream/internal/live"
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
	sId := a.sessionMgr.CurrentID(ctx, c.RoomID)
	cancel()

	outMsg := NewChatMessage(c.UserID, c.RoomID, c.Username, content, sId)

	a.manager.BroadcastToRoom(c.RoomID, outMsg)

	a.manager.PersistMsg(outMsg)
}
