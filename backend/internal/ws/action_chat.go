package ws

import (
	"strings"
	"unicode/utf8"
)

const maxChatContentLength = 200

type ChatAction struct {
	manager *Manager
}

func NewChatAction(m *Manager) *ChatAction {
	return &ChatAction{manager: m}
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

	a.manager.PersistMsg(outMsg)
}
