package ws

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
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
	// 限流放在最前面,早于内容校验。
	//
	// 为什么不等校验完再扣令牌:空内容的帧同样已经付过 ReadJSON +
	// Dispatch 的代价。如果「空消息不扣令牌」,刷屏脚本只要发空帧
	// 就能绕开整套限流 —— 把免费通道留给攻击者是没有意义的。
	if !c.chatLimiter.allow(time.Now()) {
		metrics.WSDropped.WithLabelValues("rate_limited").Inc()
		c.SendMsgOnlyOne(NewErrorMessage(c.RoomID, "发送太快了,请稍后再试"))
		return
	}

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
