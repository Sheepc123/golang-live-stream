package ws

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
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
	pool    *BroadcastPool
	connWg  sync.WaitGroup
	rdb     *redis.Client
	pubsub  *redis.PubSub
	events  *roomEventBuffer
	sink    MsgSink
	subDone chan struct{}

	// ---------- 连接额度 ----------
	//
	// 这三组状态刻意不复用上面那把 mu。
	//
	// mu 保护的是 rooms,而 rooms 在 deliver 里被每一次广播读取 ——
	// 那是全项目最热的锁。握手/断开是另一条频率完全不同的路径,
	// 把它塞进同一把锁,一次连接风暴就会直接拖慢所有房间的广播。
	// 「按访问模式分锁」,不是按数据所属的结构体分锁。

	// maxConns 全局连接上限,0 = 不限。创建后只读,不需要同步。
	maxConns int64

	// curConns 当前占用的额度。用 atomic 而不是 mutex:
	// 它只需要一个「检查并加一」的原子操作,上不上锁的语义是一样的,
	// 但 atomic 在高并发握手下没有阻塞和唤醒的开销。
	curConns atomic.Int64

	// maxConnsPerUser 单用户并发连接上限,0 = 不限。创建后只读。
	maxConnsPerUser int

	// userConns 每个用户当前的连接数,受 muUser 保护。
	// 这里没法用 atomic —— 「读 map、比较、改 map」三步不是一个原子操作。
	muUser    sync.Mutex
	userConns map[int64]int
}

const broadcastWokers = 8

var _ live.Notifier = (*Manager)(nil)

func (m *Manager) NotifyLikeCount(roomId int64, count int64) {
	m.BroadcastToRoom(roomId, NewLikeMessageCount(roomId, count))
}

// NewManager create new websocket Manager
// Start the broadcast pool
func NewManager(rdb *redis.Client, sink MsgSink, srv config.ServerConfig) *Manager {
	m := newManager()
	m.rdb = rdb
	m.sink = sink
	m.maxConns = int64(srv.MaxWSConns)
	m.maxConnsPerUser = srv.MaxConnsPerUser
	m.pool.Start()
	m.startSubsrcibe()
	return m
}

func newManager() *Manager {
	m := &Manager{
		rooms:     make(map[int64]map[*Client]bool),
		events:    newRoomEventBuffer(),
		userConns: make(map[int64]int),
	}
	// pool 只创建不 Start —— 测试直接调 deliver,不需要 worker,
	// 也就不会有 goroutine 泄漏到下一个测试里。
	m.pool = NewBroadcastPool(broadcastWokers, m.deliver)
	return m
}

// TryAcquireSlot 申请一份连接额度。返回 false 表示额度用尽,
// 调用方必须直接返回错误,不要继续 Upgrade。
//
// ── 为什么必须在 Upgrade 之前 ──
//
// 握手之后再断开,代价高得多:
//   - 已经走完了 HTTP 响应 + 协议切换
//   - 已经起了 ReadPump / WritePump 两个 goroutine(各 8 KB 起步的栈)
//   - 已经往 Redis 写过一次 viewers,还得再写一次减回去
//   - 已经进了 rooms map,广播路径要多遍历它一次
//
// 更关键的是:连接数上限的意义就是在过载时保护自己。
// 如果拒绝一条连接本身要付出接受它八成的代价,这个保护就是假的。
//
// 成功后必须配对调用 ReleaseSlot,否则额度永久泄漏。
func (m *Manager) TryAcquireSlot(userID int64) bool {
	if !m.acquireGlobal() {
		metrics.WSRejected.WithLabelValues("global_limit").Inc()
		return false
	}

	if !m.acquireUser(userID) {
		// 全局额度已经占上了,这里必须还回去。
		// 两段式申请里最容易漏的就是中途失败的回滚。
		m.releaseGlobal()
		metrics.WSRejected.WithLabelValues("user_limit").Inc()
		return false
	}

	m.connWg.Add(1)
	metrics.WSConnections.Inc()
	return true
}

// ReleaseSlot 归还额度。必须和一次成功的 TryAcquireSlot 成对出现。
func (m *Manager) ReleaseSlot(userID int64) {
	m.releaseUser(userID)
	m.releaseGlobal()
	m.connWg.Done()
	metrics.WSConnections.Dec()
}

func (m *Manager) acquireGlobal() bool {
	if m.maxConns <= 0 {
		m.curConns.Add(1)
		return true
	}

	// CAS 循环,而不是「先 Add 再判断、超了再 Add(-1)」。
	//
	// 后者在并发下会短暂突破上限:1000 条握手同时 Add,
	// 计数会先冲到 1000 才各自回退。上限设成 6 万时,
	// 那个「短暂」足够真的把内存打爆 —— 保护措施不能有超调窗口。
	for {
		cur := m.curConns.Load()
		if cur >= m.maxConns {
			return false
		}
		if m.curConns.CompareAndSwap(cur, cur+1) {
			return true
		}
		// CAS 失败 = 别人抢先改了,重新读一次再试。
	}
}

func (m *Manager) releaseGlobal() {
	m.curConns.Add(-1)
}

func (m *Manager) acquireUser(userID int64) bool {
	if m.maxConnsPerUser <= 0 {
		return true
	}

	m.muUser.Lock()
	defer m.muUser.Unlock()

	if m.userConns[userID] >= m.maxConnsPerUser {
		return false
	}
	m.userConns[userID]++
	return true
}

func (m *Manager) releaseUser(userID int64) {
	if m.maxConnsPerUser <= 0 {
		return
	}

	m.muUser.Lock()
	defer m.muUser.Unlock()

	n := m.userConns[userID] - 1
	if n <= 0 {
		// 归零必须删 key,不能留一个 0 在里面。
		// 否则这个 map 会随「历史上登录过的用户数」无限增长 ——
		// 这是长期运行的进程里最典型的一类内存泄漏:
		// 每个条目都很小,但永远不会被回收。
		delete(m.userConns, userID)
		return
	}
	m.userConns[userID] = n
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
		<-m.subDone
	}
	m.pool.Stop()

}

// Asynchronously persist message through Kafka.
func (m *Manager) PersistMsg(msg Message) {
	m.sink.Persist(msg)
}

// get the rooms which have at least one wbsocket connection.
func (m *Manager) connectedRoomsIDs() []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	roomIDs := make([]int64, 0, len(m.rooms))

	for roomId := range m.rooms {
		roomIDs = append(roomIDs, roomId)
	}
	return roomIDs
}

// broadcast the marshaled data to clients in specfied room without redis.
// false
func (m *Manager) submitbylocal(roomId int64, msg Message) bool {
	payload, err := json.Marshal(msg)
	if err != nil {
		logger.L().Error("aggregate marshal fail",
			zap.Int64("room_id", roomId),
			zap.String("type", msg.Type),
			zap.Error(err),
		)
		return false
	}

	return m.pool.Submit(roomId, msg.Type, payload)
}

func (m *Manager) NoteJoin(roomId int64, username string) {
	m.events.addJoin(roomId, username)
}

func (m *Manager) NoteLeave(roomId int64, username string) {
	m.events.addLeave(roomId, username)
}

func (m *Manager) drainRoomEvents() map[int64]*roomEvents {
	return m.events.drainAll()
}
