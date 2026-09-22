package ws

import (
	"context"
	"sync"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"go.uber.org/zap"
)

// Aggregator periodically(aggregateInterval) pushes room statistics,such as online and like counts,
// to clients connected to this instance.
//
// It submits local delivery tasks through pool.submit instead of using
// BroadcastToRoom,which publishes messages through redis.

const (
	aggregateInterval = time.Second
	aggregateTimeout  = 700 * time.Millisecond

	forceFullPushEvery = 10
)

// *Manager implement.
type interManager interface {
	connectedRoomsIDs() []int64
	submitbylocal(roomId int64, msg Message) bool
	drainRoomEvents() map[int64]*roomEvents
}

// *Live.SessionManager implement.
type interSessionManager interface {
	EvictExpiredSessions()
	BatchStats(ctx context.Context, roomIDs []int64) ([]live.RoomStat, error)
}

type roomStat struct {
	onlinecount int64
	likecount   int64
}

type Aggregator struct {
	mgr      interManager
	smgr     interSessionManager
	last     map[int64]roomStat
	wg       sync.WaitGroup
	stop     chan struct{}
	stoponce sync.Once
	round    uint64
}

func NewAggregator(m interManager, sm interSessionManager) *Aggregator {
	return &Aggregator{
		mgr:  m,
		smgr: sm,
		last: make(map[int64]roomStat),
		stop: make(chan struct{}),
	}
}

func (a *Aggregator) Start() {
	a.wg.Add(1)
	go a.run()
}

func (a *Aggregator) run() {
	defer a.wg.Done()
	ticker := time.NewTicker(aggregateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.tick()

		case <-a.stop:
			return
		}
	}
}

// tick() put onlinecount and likecount through submitbylocal.
func (a *Aggregator) tick() {
	activeRoomIds := a.mgr.connectedRoomsIDs()

	if len(activeRoomIds) == 0 {
		if len(a.last) > 0 {
			a.last = make(map[int64]roomStat)
		}
		// a.mgr.
		a.smgr.EvictExpiredSessions()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), aggregateTimeout)
	stats, err := a.smgr.BatchStats(ctx, activeRoomIds)
	cancel()

	if err != nil {
		logger.L().Warn("aggregate batch stats fail",
			zap.Int("rooms", len(activeRoomIds)),
			zap.Error(err),
		)
		return
	}

	a.round++
	force := a.round%forceFullPushEvery == 0
	next := make(map[int64]roomStat, len(activeRoomIds))

	for _, st := range stats {
		prev, ok := a.last[st.RoomID]
		if !ok {
			prev = roomStat{onlinecount: -1, likecount: -1}
		}

		cur := prev

		if force || st.OnlineCount != prev.onlinecount {
			// 只有真的投递出去了,才认为这个值「已推送」。
			//
			// 投递失败却更新脏标记,是这类聚合器最隐蔽的 bug:
			// 下一秒因为「值没变」而跳过,房间计数永久停在旧值,
			// 而且偏偏发生在队列满(高负载)时 —— 最需要数据准的时候。
			if a.mgr.submitbylocal(st.RoomID, NewOnlineCountMessage(st.RoomID, st.OnlineCount)) {
				cur.onlinecount = st.OnlineCount
			}
		}

		if force || st.LikeCount != prev.likecount {
			if a.mgr.submitbylocal(st.RoomID, NewLikeMessageCount(st.RoomID, st.LikeCount)) {
				cur.likecount = st.LikeCount
			}
		}

		next[st.RoomID] = cur
	}
	a.last = next
	a.flushRoomEvents()
	a.smgr.EvictExpiredSessions()

}

func (a *Aggregator) flushRoomEvents() {
	events := a.mgr.drainRoomEvents()

	for roomId, e := range events {
		if e.empty() {
			continue
		}
		// 房间可能刚好在这一瞬间空了,deliver 会直接返回。
		// 为这点浪费维护一个 activeRooms 的 set 不划算。
		//
		// 这里刻意不检查 submitbylocal 的返回值:drainAll 已经把事件
		// 取走了,队列满就等于这批「XX 进入了直播间」彻底丢失,不重试。
		// 和计数的处理正好相反 —— 状态可以重推(下一秒的值覆盖上一秒),
		// 事件不行(补发一条 10 秒前的「XX 进来了」只会让人困惑)。
		// 进出场提示是装饰性信息,丢了用户无感;计数错了用户一眼就看出来。
		a.mgr.submitbylocal(roomId, newRoomEventMessage(roomId, e))
	}
}

func (a *Aggregator) Stop() {
	a.stoponce.Do(func() {
		close(a.stop)
	})
	a.wg.Wait()
}
