package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// broadcastPattern "ws:room:*" can match ws:room:1,ws:room:2..... all room at once
const broadcastPattern = "ws:room:*"

// broadcastChannelString can return string "ws:room:roomId"
func broadcastChannelString(roomId int64) string {
	return fmt.Sprintf("ws:room:%d", roomId)
}

// redis broadcast the marshaled data to all active subscribers.
func (m *Manager) publish(roomId int64, msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("broadcast marshal fail : %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := m.rdb.Publish(ctx, broadcastChannelString(roomId), data).Err(); err != nil {
		log.Printf("redis failed to publish (roomId = %d) : %v", roomId, err)
		return
	}
}

// startSubsriber start the pattern subscription and groutine continuously consumes
func (m *Manager) startSubsrcibe() {
	m.pubsub = m.rdb.PSubscribe(context.Background(), broadcastPattern)

	go m.subscribeLoop()
}

func (m *Manager) subscribeLoop() {
	ch := m.pubsub.Channel()

	for redisMsg := range ch {
		var msg Message

		if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
			log.Printf("redis failed to unmarshal (channel == %s) : %v", redisMsg.Channel, err)
			continue
		}
		m.pool.Submit(msg.RoomID, msg)
	}
}
