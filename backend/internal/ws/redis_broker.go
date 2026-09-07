package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"go.uber.org/zap"
)

// broadcastPattern "ws:room:*" can match ws:room:1,ws:room:2..... all room at once
const (
	broadcastPattern = "ws:room:*"
	broadcastPrefix  = "ws:room:"
)

// broadcastChannelString can return string "ws:room:roomId:msgType"
func broadcastChannelString(roomId int64, msgType string) string {
	return fmt.Sprintf("ws:room:%d:%s", roomId, msgType)
}

func getBroadcastChannel(channel string) (roomId int64, msgType string, ok bool) {
	rest, found := strings.CutPrefix(channel, broadcastPrefix)

	if !found {
		return 0, "", false
	}

	idstr, msgType, found := strings.Cut(rest, ":")

	if !found {
		return 0, "", false
	}

	id, err := strconv.ParseInt(idstr, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, msgType, true
}

// redis broadcast the marshaled data to all active subscribers.
func (m *Manager) publish(roomId int64, msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.L().Error("broadcast marshal fail",
			zap.Int64("room_id", roomId),
			zap.String("type", msg.Type),
			zap.Error(err),
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := m.rdb.Publish(ctx, broadcastChannelString(roomId, msg.Type), data).Err(); err != nil {
		logger.L().Error("redis publish fail",
			zap.Int64("room_id", roomId),
			zap.Error(err),
		)
	}
}

// startSubsriber start the pattern subscription and groutine continuously consumes
func (m *Manager) startSubsrcibe() {
	m.pubsub = m.rdb.PSubscribe(context.Background(), broadcastPattern)
	m.subDone = make(chan struct{})
	go m.subscribeLoop()
}

func (m *Manager) subscribeLoop() {
	ch := m.pubsub.Channel()
	defer close(m.subDone)
	for redisMsg := range ch {
		roomId, msgtype, ok := getBroadcastChannel(redisMsg.Channel)
		if !ok {
			logger.L().Error("unrecognized broadcast channel",
				zap.String("channel", redisMsg.Channel),
			)
			continue

		}
		m.pool.Submit(roomId, msgtype, []byte(redisMsg.Payload))
	}
}
