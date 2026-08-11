package ws

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Manager handle the all websockets connection.
// It is responsble for :
// 1. tracking which clients are connected to each room
// 2. Registering and unregistering clients
// 3. Broadcasting the message to the client s in the same room

type Manager struct {

	// // rooms[1] = {
	// 	ClientA : true,
	// 	ClientB : true,
	// }
	// map[*Client]bool is used as a set
	rooms map[int64]map[*Client]bool

	// mu protect rooms
	// websocket concurrency situation:
	// User A  join the room
	// User B  leave the room
	// User C  send the content
	mu sync.RWMutex

	// Broadcast pool
	pool   *BroadcastPool
	connWg sync.WaitGroup
	rdb    *redis.Client
	pubsub *redis.PubSub

	producer *infra.KafkaProducer
}

const broadcastWokers = 8

var _ live.Notifier = (*Manager)(nil)

func (m *Manager) NotifyLikeCount(roomId int64, count int64) {
	m.BroadcastToRoom(roomId, NewLikeMessageCount(roomId, count))
}

// NewManager create new websocket Manager
// Start the broadcast pool
func NewManager(rdb *redis.Client, pd *infra.KafkaProducer) *Manager {
	m := newManager()
	m.rdb = rdb
	m.producer = pd
	m.pool.Start()
	m.startSubsrcibe()
	return m
}

func newManager() *Manager {
	m := &Manager{
		rooms: make(map[int64]map[*Client]bool),
	}
	// pool 只创建不 Start —— 测试直接调 deliver,不需要 worker,
	// 也就不会有 goroutine 泄漏到下一个测试里。
	m.pool = NewBroadcastPool(broadcastWokers, m.deliver)
	return m
}

// TrackConn /UnTrackConn are called by WSHandler when a Websocket Connection is established or closed.
func (m *Manager) TrackConn() {
	m.connWg.Add(1)
	metrics.WSConnections.Inc()
}
func (m *Manager) UnTrackConn() {
	m.connWg.Done()
	metrics.WSConnections.Dec()
}

// deliver broadcasts a message to all clients in the given room.
// Sends are non-blocking: if a client's send is full, the message is dropped.
func (m *Manager) deliver(roomId int64, msgType string, payload []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	clients, ok := m.rooms[roomId]
	if !ok {
		return
	}

	start := time.Now()
	down := metrics.WSMessages.WithLabelValues("down", msgType)

	// Count the number of broadcasting and dropped message.
	var sent, dropped int

	for client := range clients {
		select {
		case client.Send <- payload:
			sent++
		default:
			dropped++
		}
	}
	if sent > 0 {
		down.Add(float64(sent))
	}

	if dropped > 0 {
		metrics.WSDropped.WithLabelValues("client_slow").Add(float64(dropped))

		if logger.DebugEnabled() {
			logger.L().Debug("slow clients dropped message",
				zap.Int64("room_id", roomId),
				zap.String("type", msgType),
				zap.Int("dropped", dropped),
				zap.Int("sent", sent),
			)
		}
	}
	metrics.BroadcastDuration.Observe(time.Since(start).Seconds())
}

// Register add a new client to the corresponding live stream room.
func (m *Manager) Register(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.rooms[client.RoomID]

	if !ok {
		m.rooms[client.RoomID] = make(map[*Client]bool)
	}

	m.rooms[client.RoomID][client] = true

}

// Unregister remove a client to the corresponding live stream room
func (m *Manager) Unregister(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	clients, ok := m.rooms[client.RoomID]

	if !ok {
		return
	}

	if _, ok := clients[client]; ok {
		delete(clients, client)
		close(client.Send)
	}

	if len(clients) == 0 {
		delete(m.rooms, client.RoomID)
	}

}

// BroadcastToRoom use redis Publish
func (m *Manager) BroadcastToRoom(roomId int64, msg Message) {
	m.publish(roomId, msg)
}

// CloseALL close all the client connections
func (m *Manager) CloseAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, clients := range m.rooms {
		for c := range clients {
			c.Close()
		}
	}
}

func (m *Manager) ShutDown() {

	m.CloseAll()
	m.connWg.Wait()
	if m.pubsub != nil {
		m.pubsub.Close()
	}
	m.pool.Stop()

}

// Asynchronously persist message through Kafka.
func (m *Manager) PersistMsg(msg Message) {
	data, err := json.Marshal(msg)

	if err != nil {
		logger.L().Error("persist marshal fail",
			zap.Int64("room_id", msg.RoomID),
			zap.Int64("user_id", msg.UserID),
			zap.Error(err),
		)
		return
	}

	m.producer.Publish(strconv.FormatInt(msg.UserID, 10), data)

}
