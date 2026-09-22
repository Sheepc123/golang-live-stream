package ws

import (
	"context"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"go.uber.org/zap"
)

type LikeAction struct {
	manager     *Manager
	likecounter *live.LikeCounter
	sessionMgr  *live.SessionManager

	// instant 为 true = 实验 C 对照组:每次点赞都广播当前总数。
	// 生产路径为 false,只做 INCR,推送交给 Aggregator 每秒一次。
	instant bool
}

func NewLikeAction(m *Manager, lc *live.LikeCounter, sm *live.SessionManager, instant bool) *LikeAction {
	return &LikeAction{
		manager:     m,
		likecounter: lc,
		sessionMgr:  sm,
		instant:     instant,
	}
}

func (a *LikeAction) Execute(c *Client, m Message) {
	// 点赞被限流时「静默丢弃」,不回错误消息 —— 和弹幕刻意相反。
	//
	// 用户点赞是在连点,每次被拒都弹一条报错,一秒能刷出十几条噪音;
	// 更糟的是每条报错本身就是一次下行投递 ——
	// 限流的目的是省流量,它自己绝不能变成流量来源。
	//
	// 弹幕不一样:那是一次明确的、用户预期有回执的提交动作,
	// 悄悄吞掉会让人以为是网络坏了,反而去疯狂重试。
	if !c.likeLimiter.allow(time.Now()) {
		metrics.WSDropped.WithLabelValues("rate_limited").Inc()
		return
	}

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

	if a.instant {
		a.manager.BroadcastToRoom(c.RoomID, NewLikeMessageCount(c.RoomID, total))
	}
}
