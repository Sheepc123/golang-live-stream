package ws

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	dto "github.com/prometheus/client_model/go"
)

// counterValue 读一个 Prometheus Counter 的当前值。
//
// 不用 prometheus/testutil:它会额外拉进两个仅测试用的间接依赖。
// Counter 自己实现了 Write(*dto.Metric),直接读就行。
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.WSDropped.WithLabelValues(name).Write(&m); err != nil {
		t.Fatalf("read counter %q: %v", name, err)
	}
	return m.GetCounter().GetValue()
}

// 同一房间的 job 必须始终落到同一条队列 —— 这是「同房间消息顺序」的唯一保证。
//
// 不 Start:worker 不跑,job 留在队列里,直接数每条队列的长度。
func TestBroadcastPoolRoutesSameRoomToSameQueue(t *testing.T) {
	const workers = 4
	p := NewBroadcastPool(workers, func(int64, string, []byte) {
		t.Fatal("deliver must not run: pool was never started")
	})

	const room = int64(6)
	const jobs = 10
	for i := range jobs {
		if !p.Submit(room, MessageTypeChat, []byte("x")) {
			t.Fatalf("Submit #%d returned false on an empty queue", i+1)
		}
	}

	want := int(room % workers)
	for i, q := range p.queues {
		got := len(q)
		switch {
		case i == want && got != jobs:
			t.Fatalf("queue %d has %d jobs, want %d", i, got, jobs)
		case i != want && got != 0:
			t.Fatalf("queue %d has %d jobs, want 0 (room %d leaked across queues)", i, got, room)
		}
	}
}

// 不同房间要分散到不同队列,否则多 worker 就是摆设。
func TestBroadcastPoolSpreadsRoomsAcrossQueues(t *testing.T) {
	const workers = 4
	p := NewBroadcastPool(workers, func(int64, string, []byte) {})

	for room := range int64(workers) {
		p.Submit(room, MessageTypeChat, nil)
	}
	for i, q := range p.queues {
		if got := len(q); got != 1 {
			t.Fatalf("queue %d has %d jobs, want exactly 1", i, got)
		}
	}
}

// 队列满时 Submit 必须立刻返回 false,绝不能阻塞。
//
// 调用方是 Redis 订阅循环和 Aggregator。它们一旦被堵住,
// 所有房间的广播都停,不只是这个满了的房间。
func TestBroadcastPoolSubmitDropsWhenQueueFull(t *testing.T) {
	p := NewBroadcastPool(1, func(int64, string, []byte) {})

	capacity := cap(p.queues[0])
	for i := range capacity {
		if !p.Submit(1, MessageTypeChat, nil) {
			t.Fatalf("Submit #%d returned false before the queue was full (cap %d)", i+1, capacity)
		}
	}

	before := counterValue(t, "queue_full")

	done := make(chan bool, 1)
	go func() { done <- p.Submit(1, MessageTypeChat, nil) }()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("Submit returned true on a full queue; the job was silently lost or the cap is wrong")
		}
	case <-time.After(time.Second):
		t.Fatal("Submit blocked on a full queue")
	}

	if got := counterValue(t, "queue_full") - before; got != 1 {
		t.Fatalf("live_ws_dropped_total{reason=queue_full} increased by %v, want 1", got)
	}
}

// worker 真的跑起来时:同一房间严格按提交顺序投递,不同房间互不干扰,
// Stop 之后队列里剩下的 job 都要被送完而不是丢掉。
func TestBroadcastPoolPreservesOrderWithinRoom(t *testing.T) {
	var (
		mu       sync.Mutex
		received = map[int64][]uint32{}
	)
	p := NewBroadcastPool(4, func(roomId int64, _ string, payload []byte) {
		mu.Lock()
		received[roomId] = append(received[roomId], binary.LittleEndian.Uint32(payload))
		mu.Unlock()
	})
	p.Start()

	const perRoom = 200
	rooms := []int64{5, 6, 9} // 5%4=1, 6%4=2, 9%4=1:两个房间共用一个 worker
	for i := range uint32(perRoom) {
		for _, room := range rooms {
			payload := make([]byte, 4)
			binary.LittleEndian.PutUint32(payload, i)
			if !p.Submit(room, MessageTypeChat, payload) {
				t.Fatalf("Submit(room %d, #%d) dropped", room, i)
			}
		}
	}

	// Stop 关闭队列并等 worker 排空 —— 返回后所有 job 都应已投递
	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	for _, room := range rooms {
		seq := received[room]
		if len(seq) != perRoom {
			t.Fatalf("room %d received %d jobs, want %d (Stop dropped queued jobs)", room, len(seq), perRoom)
		}
		for i, v := range seq {
			if v != uint32(i) {
				t.Fatalf("room %d job #%d = %d: order broken", room, i, v)
			}
		}
	}
}

// workers <= 0 是配置错误,要兜成 1 而不是 panic(roomId % 0)。
func TestBroadcastPoolMinimumOneWorker(t *testing.T) {
	p := NewBroadcastPool(0, func(int64, string, []byte) {})
	if len(p.queues) != 1 {
		t.Fatalf("queues = %d, want 1", len(p.queues))
	}
	if !p.Submit(12345, MessageTypeChat, nil) {
		t.Fatal("Submit failed on a single-worker pool")
	}
}
