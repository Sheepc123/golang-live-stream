package ws

import (
	"strings"
	"unicode/utf8"

	"github.com/Sheepc123/golang-live-stream/internal/service"
)

const maxChatContentLength = 200

type ChatAction struct {
	manager    *Manager
	MsgService *service.MsgService
}

func NewChatAction(m *Manager, s *service.MsgService) *ChatAction {
	return &ChatAction{
		manager:    m,
		MsgService: s,
	}
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

	outMsg := NewChatMessage(c.UserID, c.RoomID, c.Username, content)

	a.manager.BroadcastToRoom(c.RoomID, outMsg)

	saveMessageAsynv(a.MsgService, outMsg)
}
