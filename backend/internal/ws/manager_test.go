package ws

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// newTestClient 造一个只有 Send channel 的 Client。
//
// Conn 故意留 nil:这些测试只覆盖 Manager 的内存逻辑,
// 不碰 ReadPump / WritePump / Close,所以永远不会解引用 Conn。
// 哪天测试挂在 nil pointer 上,说明它越界测到了不该测的东西。
func newTestClient(roomID int64, buf int) *Client {
	return &Client{
		RoomID: roomID,
		Send:   make(chan []byte, buf),
	}
}

// testPayload 把 Message 序列化成 deliver 需要的 payload。
//
// 注意:测试里必须自己序列化了 —— 这正是改造的意义,
// 「序列化」这件事被明确地移出了投递路径。
func testPayload(t testing.TB, msg Message) []byte {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	return data
}

// 核心测试:Register / Unregister / deliver 三者并发。
//
// 它要证明的唯一一件事:deliver 里的 `client.Send <- payload`
// 永远不会撞上 Unregister 里的 `close(client.Send)`。
// 撞上就是 panic,而 BroadcastPool 的 worker 没有 recover ——
// 那是整个进程崩掉,不是断一个连接。
func TestManagerConcurrentRegisterDeliverUnregister(t *testing.T) {
	m := newManager()

	const (
		rooms          = 4
		clientsPerRoom = 25
		broadcasters   = 8
		messages       = 300
	)

	var wg sync.WaitGroup

	// 第一批:不断进出房间的客户端
	for r := int64(0); r < rooms; r++ {
		for i := 0; i < clientsPerRoom; i++ {
			wg.Add(1)
			go func(roomID int64) {
				defer wg.Done()
				c := newTestClient(roomID, 8)

				// 模拟 WritePump:持续排空 Send。
				// for range 在 channel 被 close 后自然退出 ——
				// 这正是生产里 WritePump 感知「我被注销了」的方式。
				drained := make(chan struct{})
				go func() {
					defer close(drained)
					for range c.Send {
					}
				}()

				m.Register(c)
				time.Sleep(time.Millisecond) // 留出被广播命中的窗口
				m.Unregister(c)
				<-drained // 确认排空者已退出再收工
			}(r)
		}
	}

	// 第二批:并发往所有房间广播
	for b := 0; b < broadcasters; b++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < messages; i++ {
				for r := int64(0); r < rooms; r++ {
					payload := testPayload(t, NewSystemMessage(r, "ping"))
					m.deliver(r, MessageTypeSystem, payload)
				}
			}
		}()
	}

	wg.Wait()

	// 所有人都注销了,rooms 必须被清空 —— 否则 map 随房间数无限增长
	if len(m.rooms) != 0 {
		t.Fatalf("rooms not cleaned up: got %d entries, want 0", len(m.rooms))
	}
}

// deliver 必须非阻塞:客户端太慢就丢消息,绝不能拖住广播。
//
// 这是本项目刻意的取舍 —— 宁可丢弹幕,不可让一个卡死的客户端
// 把整个 worker(以及它负责的所有房间)堵住。
func TestManagerDeliverDropsWhenSendIsFull(t *testing.T) {
	m := newManager()

	const buf = 2
	c := newTestClient(1, buf) // 故意不启排空者,模拟卡死的客户端
	m.Register(c)

	payload := testPayload(t, NewSystemMessage(1, "x"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < buf+5; i++ {
			m.deliver(1, MessageTypeSystem, payload)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// 走到这里说明 deliver 的 select default 分支没了 ——
		// 一个慢客户端就能永久堵死一个 BroadcastPool worker
		t.Fatal("deliver blocked on a full Send channel")
	}

	if got := len(c.Send); got != buf {
		t.Fatalf("Send buffer = %d, want %d (extra sends should have been dropped)", got, buf)
	}
}

// 所有客户端必须拿到同一个底层数组 —— 这就是「一次序列化」的直接证明。
//
// 这个断言看起来在测实现细节,但它守的是一条真正的契约:
// 一旦有人在投递路径里加了 copy 或重新 Marshal,
// 6 万连接的 CPU 就悄悄涨回去了,而功能测试完全看不出来。
func TestManagerDeliverSharesOnePayload(t *testing.T) {
	m := newManager()

	a := newTestClient(1, 1)
	b := newTestClient(1, 1)
	m.Register(a)
	m.Register(b)

	payload := testPayload(t, NewSystemMessage(1, "same"))
	m.deliver(1, MessageTypeSystem, payload)

	got1 := <-a.Send
	got2 := <-b.Send

	// &slice[0] 取底层数组首元素地址:相等即同一份内存
	if &got1[0] != &got2[0] {
		t.Fatal("clients received different backing arrays; payload was copied or re-serialized")
	}
}

// 重复 Unregister 必须是 no-op。
//
// 现在靠 `if _, ok := clients[client]; ok` 这层判断保证。
// 去掉它就是 close 一个已关闭的 channel —— 同样 panic。
func TestManagerUnregisterTwiceIsNoop(t *testing.T) {
	m := newManager()
	c := newTestClient(1, 1)

	m.Register(c)
	m.Unregister(c)
	m.Unregister(c) // 没有那层保护这里就 panic

	if len(m.rooms) != 0 {
		t.Fatalf("rooms = %d entries, want 0", len(m.rooms))
	}
}

// 房间只在最后一人离开时才删除 —— 早删会让还在场的人收不到消息。
func TestManagerRoomRemovedOnlyWhenEmpty(t *testing.T) {
	m := newManager()
	a := newTestClient(1, 1)
	b := newTestClient(1, 1)

	m.Register(a)
	m.Register(b)

	m.Unregister(a)
	if _, ok := m.rooms[1]; !ok {
		t.Fatal("room 1 was removed while client b is still connected")
	}

	m.Unregister(b)
	if _, ok := m.rooms[1]; ok {
		t.Fatal("room 1 should be removed after the last client left")
	}
}

// 频道名编解码必须能往返 —— 它现在承载了路由信息,错了消息就发错房间。
func TestBroadcastChannelRoundTrip(t *testing.T) {
	cases := []struct {
		roomId  int64
		msgType string
	}{
		{1, MessageTypeChat},
		{99999, MessageTypeLikeCount},
		{0, MessageTypeSystem},
	}

	for _, tc := range cases {
		ch := broadcastChannelString(tc.roomId, tc.msgType)
		gotRoom, gotType, ok := getBroadcastChannel(ch)
		if !ok || gotRoom != tc.roomId || gotType != tc.msgType {
			t.Errorf("round trip %q: got (%d, %q, %v), want (%d, %q, true)",
				ch, gotRoom, gotType, ok, tc.roomId, tc.msgType)
		}
	}

	// 非法输入必须被拒绝,而不是解析出一个错的 roomId
	for _, bad := range []string{"", "ws:room:", "ws:room:abc:chat", "other:5:chat", "ws:room:5"} {
		if _, _, ok := getBroadcastChannel(bad); ok {
			t.Errorf("parseBroadcastChannel(%q) = ok, want not ok", bad)
		}
	}
}

// BenchmarkManagerDeliver 量化改造效果,也是 newplan 里那条
// 「deliver 零分配」验收标准的测量手段。
//
// 跑法:go test -bench=Deliver -benchmem ./internal/ws/
func BenchmarkManagerDeliver(b *testing.B) {
	m := newManager()

	const clients = 1000
	for i := 0; i < clients; i++ {
		c := newTestClient(1, sendBuffer)
		go func() {
			for range c.Send {
			}
		}()
		m.Register(c)
	}

	payload := testPayload(b, NewChatMessage(1, 1, "user", "hello world", 1))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.deliver(1, MessageTypeChat, payload)
	}
}
