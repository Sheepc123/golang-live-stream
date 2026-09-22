package live

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis 起一个进程内的 miniredis,并返回连上它的 go-redis 客户端。
//
// 为什么用 miniredis 而不是 mock 接口:
// 这里要测的是 Lua 脚本本身 —— HINCRBY / HDEL / EXPIRE 的组合逻辑。
// mock 只能验证「调了哪个命令」,验证不了「脚本算出来的数对不对」。
// miniredis 是纯 Go 实现的 Redis,支持 EVAL,能真的把脚本跑一遍。
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return s, rdb
}

// ---------- OnlineCounter ----------

// 在线人数按「用户」去重,不按「连接」计数。
// 同一用户开两个标签页,HLEN 仍然是 1;关掉一个后仍是 1,全关才归零。
func TestOnlineCounterCountsUniqueUsers(t *testing.T) {
	s, rdb := newTestRedis(t)
	c := NewOnlineCounter(rdb)
	ctx := context.Background()

	const room = int64(1)

	if n := c.Join(ctx, room, 100); n != 1 {
		t.Fatalf("first join = %d, want 1", n)
	}
	// 同一用户第二条连接:人数不变
	if n := c.Join(ctx, room, 100); n != 1 {
		t.Fatalf("same user second join = %d, want 1 (must dedupe by user)", n)
	}
	if n := c.Join(ctx, room, 200); n != 2 {
		t.Fatalf("second user join = %d, want 2", n)
	}

	// 用户 100 关掉一个标签页:还有一条连接在,人数不变
	if n := c.Leave(ctx, room, 100); n != 2 {
		t.Fatalf("leave with one connection remaining = %d, want 2", n)
	}
	// 关掉最后一个:这才真的离开
	if n := c.Leave(ctx, room, 100); n != 1 {
		t.Fatalf("leave last connection = %d, want 1", n)
	}
	if n := c.Leave(ctx, room, 200); n != 0 {
		t.Fatalf("last user leave = %d, want 0", n)
	}

	// 最后一个字段被 HDEL 后 Redis 会自动删掉空 hash,
	// 不能留一个空 key 占着内存等 24 小时过期。
	if s.Exists(ViewersKey(room)) {
		t.Fatal("viewers hash still exists after the last user left")
	}

	// Count 走的是 HLEN,和 Join/Leave 返回值必须一致
	n, err := c.Count(ctx, room)
	if err != nil || n != 0 {
		t.Fatalf("Count after everyone left = (%d, %v), want (0, nil)", n, err)
	}
}

// 从未加入过的用户调 Leave,不能把人数减成负数,也不能留下脏字段。
//
// 这种情况真实存在:Join 时 Redis 超时返回 0,连接断开时照样会调 Leave。
func TestOnlineCounterLeaveWithoutJoinIsHarmless(t *testing.T) {
	s, rdb := newTestRedis(t)
	c := NewOnlineCounter(rdb)
	ctx := context.Background()

	if n := c.Leave(ctx, 1, 999); n != 0 {
		t.Fatalf("leave without join = %d, want 0", n)
	}
	if s.Exists(ViewersKey(1)) {
		t.Fatal("leave without join left a hash behind (negative counter leaked)")
	}
}

// 每次 Join / Leave 都要续期,否则一场直播超过 24 小时后计数会凭空消失。
func TestOnlineCounterRefreshesTTL(t *testing.T) {
	s, rdb := newTestRedis(t)
	c := NewOnlineCounter(rdb)
	ctx := context.Background()

	c.Join(ctx, 1, 100)
	if got := s.TTL(ViewersKey(1)); got != onlineTTL {
		t.Fatalf("TTL after join = %v, want %v", got, onlineTTL)
	}

	// 时间过去一半,再有人进出,TTL 必须被重置回满值
	s.FastForward(onlineTTL / 2)
	c.Join(ctx, 1, 200)
	if got := s.TTL(ViewersKey(1)); got != onlineTTL {
		t.Fatalf("TTL after second join = %v, want %v (not refreshed)", got, onlineTTL)
	}

	s.FastForward(onlineTTL / 2)
	c.Leave(ctx, 1, 200)
	if got := s.TTL(ViewersKey(1)); got != onlineTTL {
		t.Fatalf("TTL after leave = %v, want %v (not refreshed)", got, onlineTTL)
	}
}

// 峰值只升不降。
func TestOnlineCounterPeakOnlyIncreases(t *testing.T) {
	_, rdb := newTestRedis(t)
	c := NewOnlineCounter(rdb)
	ctx := context.Background()

	const session = int64(7)

	peak := func() int64 {
		t.Helper()
		n, err := c.Peak(ctx, session)
		if err != nil {
			t.Fatalf("Peak: %v", err)
		}
		return n
	}

	if got := peak(); got != 0 {
		t.Fatalf("peak before any update = %d, want 0", got)
	}

	c.UpdatePeak(ctx, session, 5)
	if got := peak(); got != 5 {
		t.Fatalf("peak after 5 = %d, want 5", got)
	}

	// 人数回落,峰值不能跟着降
	c.UpdatePeak(ctx, session, 3)
	if got := peak(); got != 5 {
		t.Fatalf("peak after drop to 3 = %d, want 5 (peak must never decrease)", got)
	}

	c.UpdatePeak(ctx, session, 9)
	if got := peak(); got != 9 {
		t.Fatalf("peak after 9 = %d, want 9", got)
	}

	// 两个哨兵值都是 no-op:sessionId=0 表示未开播,n<=0 没有意义
	c.UpdatePeak(ctx, 0, 100)
	c.UpdatePeak(ctx, session, 0)
	if got := peak(); got != 9 {
		t.Fatalf("peak after no-op updates = %d, want 9", got)
	}
	if n, _ := c.Peak(ctx, 0); n != 0 {
		t.Fatalf("peak for session 0 = %d, want 0 (must not be written)", n)
	}
}

// BatchCount 一次 pipeline 拿多个房间;不存在的房间返回 0 而不是缺项。
func TestOnlineCounterBatchCount(t *testing.T) {
	_, rdb := newTestRedis(t)
	c := NewOnlineCounter(rdb)
	ctx := context.Background()

	c.Join(ctx, 1, 100)
	c.Join(ctx, 1, 200)
	c.Join(ctx, 2, 300)

	got, err := c.BatchCount(ctx, []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("BatchCount: %v", err)
	}
	want := map[int64]int64{1: 2, 2: 1, 3: 0}
	if len(got) != len(want) {
		t.Fatalf("BatchCount = %v, want %v", got, want)
	}
	for room, n := range want {
		if got[room] != n {
			t.Fatalf("room %d = %d, want %d (full result %v)", room, got[room], n, got)
		}
	}

	// 空输入不该发任何命令
	if got, err := c.BatchCount(ctx, nil); got != nil || err != nil {
		t.Fatalf("BatchCount(nil) = (%v, %v), want (nil, nil)", got, err)
	}
}

// Redis 不可用时:写路径返回 0 不 panic,读路径把错误交给调用方决定怎么降级。
func TestOnlineCounterDegradesWhenRedisIsDown(t *testing.T) {
	s, rdb := newTestRedis(t)
	c := NewOnlineCounter(rdb)
	ctx := context.Background()

	s.Close()

	if n := c.Join(ctx, 1, 100); n != 0 {
		t.Fatalf("Join with redis down = %d, want 0", n)
	}
	if n := c.Leave(ctx, 1, 100); n != 0 {
		t.Fatalf("Leave with redis down = %d, want 0", n)
	}
	if _, err := c.Count(ctx, 1); err == nil {
		t.Fatal("Count with redis down returned nil error; caller cannot tell 0 from failure")
	}
	if _, err := c.BatchCount(ctx, []int64{1}); err == nil {
		t.Fatal("BatchCount with redis down returned nil error")
	}
}

// ---------- LikeCounter ----------

// Lua 脚本把 INCR 和 EXPIRE 合在一次往返里:计数要连续,TTL 要每次重置。
func TestLikeCounterIncrAndTTL(t *testing.T) {
	s, rdb := newTestRedis(t)
	c := NewLikeCounter(rdb)
	ctx := context.Background()

	const session = int64(42)

	// 没点过赞的场次读出来是 0,不是错误
	n, err := c.GetHistoryLike(ctx, session)
	if err != nil || n != 0 {
		t.Fatalf("GetHistoryLike before any like = (%d, %v), want (0, nil)", n, err)
	}

	for i := int64(1); i <= 3; i++ {
		if got := c.Incr(ctx, session); got != i {
			t.Fatalf("Incr #%d = %d, want %d", i, got, i)
		}
	}
	if got := s.TTL(LikeKey(session)); got != likeTTL {
		t.Fatalf("TTL after incr = %v, want %v", got, likeTTL)
	}

	n, err = c.GetHistoryLike(ctx, session)
	if err != nil || n != 3 {
		t.Fatalf("GetHistoryLike = (%d, %v), want (3, nil)", n, err)
	}

	// 滑动过期:中途再点一次,TTL 回满
	s.FastForward(likeTTL / 2)
	c.Incr(ctx, session)
	if got := s.TTL(LikeKey(session)); got != likeTTL {
		t.Fatalf("TTL after incr at half-life = %v, want %v (not refreshed)", got, likeTTL)
	}

	// 无人点赞满 24 小时后 key 消失,读出来回到 0
	s.FastForward(likeTTL + time.Second)
	n, err = c.GetHistoryLike(ctx, session)
	if err != nil || n != 0 {
		t.Fatalf("GetHistoryLike after expiry = (%d, %v), want (0, nil)", n, err)
	}
}

// Redis 挂了时点赞返回 0(记日志),不能让 ReadPump 因为一个 panic 挂掉整条连接。
func TestLikeCounterReturnsZeroWhenRedisIsDown(t *testing.T) {
	s, rdb := newTestRedis(t)
	c := NewLikeCounter(rdb)

	s.Close()

	if n := c.Incr(context.Background(), 1); n != 0 {
		t.Fatalf("Incr with redis down = %d, want 0", n)
	}
	if _, err := c.GetHistoryLike(context.Background(), 1); err == nil {
		t.Fatal("GetHistoryLike with redis down returned nil error")
	}
}
