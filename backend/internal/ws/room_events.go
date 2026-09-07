package ws

import "sync"

// 单个房间一轮内最多携带的名字数。
//
// 为什么必须封顶:6 万人涌入时,一秒内的 joined 名单能有几万个用户名 ——
// 那条聚合消息本身就有几百 KB,再乘以房间里的 6 万人就是几十 GB 的下行。
//
// 聚合解决的是「消息条数」问题,不解决「单条消息大小」问题。
// 两个问题必须分别处理,否则只是把扇出爆炸换成了带宽爆炸。
// 超出的部分只统计数量,不带名字。
const maxRoomEventNames = 20

// roomEvents 是一个房间在当前这一轮里累积的进出场事件。
type roomEvents struct {
	joined []string
	left   []string

	// 被截断掉的人数,前端可以显示成「…等 1024 人进入直播间」
	joinedOverflow int
	leftOverflow   int
}

func (e *roomEvents) empty() bool {
	return len(e.joined) == 0 && len(e.left) == 0 &&
		e.joinedOverflow == 0 && e.leftOverflow == 0
}

// roomEventBuffer 收集进出场事件,由 Aggregator 每秒取走一次。
//
// 为什么不直接广播:
//
//	6 万人进房 = 6 万条 join,每条扇出给房间里的 6 万人 = 36 亿次投递。
//	这个数字比点赞的扇出还大一个量级,而且它偏偏发生在开播瞬间 ——
//	系统最脆弱的时刻。
//
// 结论:任何会被扇出的消息都得过一遍聚合,不只是计数类的。
type roomEventBuffer struct {
	mu sync.Mutex
	m  map[int64]*roomEvents
}

func newRoomEventBuffer() *roomEventBuffer {
	return &roomEventBuffer{m: make(map[int64]*roomEvents)}
}

func (b *roomEventBuffer) addJoin(roomId int64, username string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entryLocked(roomId)
	if len(e.joined) < maxRoomEventNames {
		e.joined = append(e.joined, username)
		return
	}
	e.joinedOverflow++
}

func (b *roomEventBuffer) addLeave(roomId int64, username string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entryLocked(roomId)
	if len(e.left) < maxRoomEventNames {
		e.left = append(e.left, username)
		return
	}
	e.leftOverflow++
}

// entryLocked 取或建一个房间的事件槽。调用方必须已持有 mu。
func (b *roomEventBuffer) entryLocked(roomId int64) *roomEvents {
	e, ok := b.m[roomId]
	if !ok {
		e = &roomEvents{}
		b.m[roomId] = e
	}
	return e
}

// drainAll 取走所有房间累积的事件,并把缓冲重置。
//
// 整体换一个新 map,而不是逐个房间去取 —— 一次加锁搞定,
// 不用在 Aggregator 的循环里反复抢一把被所有连接争用的锁。
func (b *roomEventBuffer) drainAll() map[int64]*roomEvents {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.m) == 0 {
		return nil
	}

	out := b.m
	b.m = make(map[int64]*roomEvents)
	return out
}
