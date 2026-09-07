package ws

import (
	"context"
	"errors"
	"testing"

	"github.com/Sheepc123/golang-live-stream/internal/live"
)

var (
	_ interManager        = (*fakeTarget)(nil)
	_ interSessionManager = (*fakeSource)(nil)
)

// fakeTarget 记录所有投递,并可以被指定「投递一律失败」。
type fakeTarget struct {
	rooms []int64

	// failSubmit 为 true 时 submitLocal 一律返回 false,
	// 模拟 BroadcastPool 队列满、消息被丢弃。
	failSubmit bool
	sent       []Message
	events     map[int64]*roomEvents
}

func (f *fakeTarget) connectedRoomsIDs() []int64 { return f.rooms }

func (f *fakeTarget) submitbylocal(roomId int64, msg Message) bool {
	if f.failSubmit {
		return false
	}
	f.sent = append(f.sent, msg)
	return true
}

func (f *fakeTarget) drainRoomEvents() map[int64]*roomEvents {
	out := f.events
	f.events = nil
	return out
}

func (f *fakeTarget) reset() { f.sent = nil }

func (f *fakeTarget) countByType(msgType string) int {
	n := 0
	for _, m := range f.sent {
		if m.Type == msgType {
			n++
		}
	}
	return n
}

// fakeSource 返回固定统计值,可被指定返回错误。
type fakeSource struct {
	stats []live.RoomStat
	err   error
}

func (f *fakeSource) BatchStats(ctx context.Context, roomIds []int64) ([]live.RoomStat, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.stats, nil
}

func (f *fakeSource) EvictExpiredSessions() {}

func oneRoom() (*fakeTarget, *fakeSource) {
	return &fakeTarget{rooms: []int64{1}},
		&fakeSource{stats: []live.RoomStat{
			{RoomID: 1, SessionID: 7, OnlineCount: 3, LikeCount: 10},
		}}
}

// 新房间的第一轮必须无条件推送 —— 客户端刚连上,什么都不知道。
func TestAggregatorFirstTickPushesEverything(t *testing.T) {
	target, source := oneRoom()

	a := NewAggregator(target, source)
	a.tick()

	if got := target.countByType(MessageTypeOnlineCount); got != 1 {
		t.Fatalf("online_count pushes = %d, want 1", got)
	}
	if got := target.countByType(MessageTypeLikeCount); got != 1 {
		t.Fatalf("like_count pushes = %d, want 1", got)
	}
}

// 值没变就不推 —— 静默房间必须零下行流量,这是聚合省下的主要成本。
func TestAggregatorSkipsUnchangedValues(t *testing.T) {
	target, source := oneRoom()

	a := NewAggregator(target, source)
	a.tick()
	target.reset()

	a.tick()
	a.tick()

	if got := len(target.sent); got != 0 {
		t.Fatalf("sent %d messages for unchanged values, want 0", got)
	}
}

// 核心用例:投递失败时不能更新脏标记。
//
// 没有这个保证,一次队列满就会让房间的计数永久停在旧值 ——
// 因为下一轮「值没变」会直接跳过,再也不会重推。
func TestAggregatorRetriesAfterSubmitFailure(t *testing.T) {
	target, source := oneRoom()
	target.failSubmit = true

	a := NewAggregator(target, source)

	// 第一轮:投递全部失败,一条都没送出去
	a.tick()
	if got := len(target.sent); got != 0 {
		t.Fatalf("sent %d messages while submit failing, want 0", got)
	}

	// 队列恢复,值依然没变
	target.failSubmit = false
	a.tick()

	// 必须重推 —— 因为上一轮根本没送达
	if got := target.countByType(MessageTypeOnlineCount); got != 1 {
		t.Fatalf("online_count after recovery = %d, want 1 "+
			"(dirty flag was updated on a failed send)", got)
	}
	if got := target.countByType(MessageTypeLikeCount); got != 1 {
		t.Fatalf("like_count after recovery = %d, want 1", got)
	}
}

// 周期性全量对账:即使值没变,每 forceFullPushEvery 轮也要重推一次。
func TestAggregatorForcesFullPush(t *testing.T) {
	target, source := oneRoom()

	a := NewAggregator(target, source)
	a.tick() // round 1:首轮全推
	target.reset()

	// 推到强制轮之前,期间不该有任何下行
	for i := 2; i < forceFullPushEvery; i++ {
		a.tick()
	}
	if got := len(target.sent); got != 0 {
		t.Fatalf("sent %d messages before the force round, want 0", got)
	}

	a.tick() // 第 forceFullPushEvery 轮
	if got := len(target.sent); got != 2 {
		t.Fatalf("force round sent %d messages, want 2 (online + like)", got)
	}
}

// 整批读取失败时必须保留脏标记,恢复后不能全量重推。
func TestAggregatorKeepsStateOnBatchError(t *testing.T) {
	target, source := oneRoom()

	a := NewAggregator(target, source)
	a.tick()
	target.reset()

	source.err = errors.New("redis down")
	a.tick()
	if got := len(target.sent); got != 0 {
		t.Fatalf("sent %d messages during batch error, want 0", got)
	}

	source.err = nil
	a.tick()
	if got := len(target.sent); got != 0 {
		t.Fatalf("sent %d messages after recovery with unchanged values, want 0 "+
			"(state was wiped on error, causing a recovery spike)", got)
	}
}

// 房间全空时清空脏标记 —— 下一个进来的人必须收到全量,不能拿旧状态比对。
func TestAggregatorResetsWhenIdle(t *testing.T) {
	target, source := oneRoom()

	a := NewAggregator(target, source)
	a.tick()
	target.reset()

	target.rooms = nil // 所有人离开
	a.tick()
	if len(a.last) != 0 {
		t.Fatalf("last = %d entries after all rooms went empty, want 0", len(a.last))
	}

	// 有人回来了,值和之前完全一样 —— 但必须当成新房间全量重推
	target.rooms = []int64{1}
	a.tick()
	if got := len(target.sent); got != 2 {
		t.Fatalf("sent %d messages to the returning room, want 2", got)
	}
}

// 进出场事件没有脏标记:每一轮有就推,推完清空,不重复推。
func TestAggregatorFlushesRoomEventsOnce(t *testing.T) {
	target, source := oneRoom()
	target.events = map[int64]*roomEvents{
		1: {joined: []string{"alice"}, left: []string{"bob"}},
	}

	a := NewAggregator(target, source)
	a.tick()

	if got := target.countByType(MessageTypeRoomEvent); got != 1 {
		t.Fatalf("room_event pushes = %d, want 1", got)
	}

	target.reset()
	a.tick()
	if got := target.countByType(MessageTypeRoomEvent); got != 0 {
		t.Fatalf("room_event re-pushed %d times after drain, want 0", got)
	}
}
