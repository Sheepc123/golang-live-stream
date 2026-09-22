package ws

import (
	"sync"
	"testing"
	"time"
)

// 令牌桶的时间依赖全部通过 allow(now) 注入,所以这些测试
// 不需要 time.Sleep,也就不会因为 CI 机器卡一下而偶发失败。
// 「把时钟变成参数」是让时间相关逻辑可测的最省事的办法。
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// 桶初始是满的:用户刚连上就该能立刻发言,而不是先等着攒令牌。
func TestTokenBucketStartsFull(t *testing.T) {
	b := newTokenBucket(2, 5)

	for i := 0; i < 5; i++ {
		if !b.allow(t0) {
			t.Fatalf("burst 消耗到第 %d 个就被拒了,应该能连发 5 个", i+1)
		}
	}
	if b.allow(t0) {
		t.Fatal("突发额度用完后还放行,上限没生效")
	}
}

// 令牌按流逝的时间线性补充。
func TestTokenBucketRefillsOverTime(t *testing.T) {
	b := newTokenBucket(2, 5) // 每秒 2 个 = 每 500ms 一个

	for i := 0; i < 5; i++ {
		b.allow(t0)
	}

	// 499ms:还差一点,不该放行
	if b.allow(t0.Add(499 * time.Millisecond)) {
		t.Fatal("不到 500ms 就补满了一个令牌,补充速率算错了")
	}

	// 再过 1ms 凑满 500ms,补到第一个令牌
	if !b.allow(t0.Add(500 * time.Millisecond)) {
		t.Fatal("满 500ms 后没有补充令牌")
	}

	// 刚花掉,又空了
	if b.allow(t0.Add(500 * time.Millisecond)) {
		t.Fatal("同一时刻放行了两个令牌")
	}
}

// 这个用例守的是最容易写错的一行:补充必须用浮点数累加。
//
// 如果 tokens 是 int,每 100ms 补充 2*0.1=0.2 个会被截断成 0,
// 桶永远攒不满 —— 限流速率会静默地变成 0,用户一条都发不出去。
// 而这种 bug 在「每秒调一次」的测试里完全看不出来。
func TestTokenBucketAccumulatesFractionalTokens(t *testing.T) {
	b := newTokenBucket(2, 5)

	for i := 0; i < 5; i++ {
		b.allow(t0)
	}

	// 分 5 次、每次 100ms 地推进时间,总共 500ms
	now := t0
	for i := 0; i < 4; i++ {
		now = now.Add(100 * time.Millisecond)
		if b.allow(now) {
			t.Fatalf("第 %d 个 100ms 就放行了,不该攒够", i+1)
		}
	}

	now = now.Add(100 * time.Millisecond)
	if !b.allow(now) {
		t.Fatal("零碎的时间片没有累加成完整令牌(tokens 被截断成整数了?)")
	}
}

// 攒令牌必须封顶。不封的话,挂机一小时回来能瞬间发几千条。
func TestTokenBucketCapsAtBurst(t *testing.T) {
	b := newTokenBucket(2, 5)

	// 空闲一小时,理论上能补 7200 个令牌
	idle := t0.Add(time.Hour)

	n := 0
	for b.allow(idle) {
		n++
		if n > 100 {
			t.Fatal("空闲后放行数量远超 burst,令牌没有封顶")
		}
	}

	if n != 5 {
		t.Fatalf("空闲一小时后放行 %d 条,应该正好是 burst=5 条", n)
	}
}

// 时间倒退(NTP 回拨之类)时不能凭空多发令牌,也不能死锁。
func TestTokenBucketIgnoresBackwardsTime(t *testing.T) {
	b := newTokenBucket(2, 5)
	for i := 0; i < 5; i++ {
		b.allow(t0)
	}

	// 时钟倒退 10 秒:elapsed 是负数,不能拿来"补充"
	if b.allow(t0.Add(-10 * time.Second)) {
		t.Fatal("时间倒退时放行了请求,elapsed 为负却仍在补充令牌")
	}
}

// ---------- 连接额度 ----------

// 全局上限:占满之后必须拒绝,归还之后必须能重新申请。
func TestManagerGlobalConnLimit(t *testing.T) {
	m := newManager()
	m.maxConns = 3

	for i := 0; i < 3; i++ {
		if !m.TryAcquireSlot(int64(i)) {
			t.Fatalf("第 %d 个额度就被拒,上限是 3", i+1)
		}
	}

	if m.TryAcquireSlot(99) {
		t.Fatal("超出全局上限仍然放行")
	}

	m.ReleaseSlot(0)
	if !m.TryAcquireSlot(99) {
		t.Fatal("归还一个额度后新连接仍然被拒,额度没有真正释放")
	}

	// 把 WaitGroup 清干净,否则它带着计数进入下一个测试
	m.ReleaseSlot(1)
	m.ReleaseSlot(2)
	m.ReleaseSlot(99)
}

// 单用户上限:同一个 userID 超额要被拒,别的用户不受影响。
func TestManagerPerUserConnLimit(t *testing.T) {
	m := newManager()
	m.maxConnsPerUser = 2

	if !m.TryAcquireSlot(7) || !m.TryAcquireSlot(7) {
		t.Fatal("用户 7 的前两条连接就被拒了")
	}
	if m.TryAcquireSlot(7) {
		t.Fatal("用户 7 的第三条连接应该被拒")
	}

	// 额度是按人算的,不能误伤别人
	if !m.TryAcquireSlot(8) {
		t.Fatal("用户 8 被用户 7 的额度影响了")
	}

	m.ReleaseSlot(7)
	if !m.TryAcquireSlot(7) {
		t.Fatal("用户 7 释放一条后无法重连")
	}

	m.ReleaseSlot(7)
	m.ReleaseSlot(7)
	m.ReleaseSlot(8)
}

// 核心回归用例:单用户额度被拒时,必须把已经占用的全局额度还回去。
//
// 两段式申请里最容易漏的就是这个中途回滚。漏了的话,
// 每一次「单用户超限」都会永久吃掉一个全局额度 ——
// 服务跑几天之后全局额度被慢慢啃光,表现是「明明没人却连不上」,
// 而且只在有人开小号刷连接时才发生,极难复现。
func TestManagerRollsBackGlobalSlotOnUserLimit(t *testing.T) {
	m := newManager()
	m.maxConns = 10
	m.maxConnsPerUser = 1

	if !m.TryAcquireSlot(1) {
		t.Fatal("第一条连接就被拒")
	}

	// 用户 1 超额,应该在 user 这一步被拒
	for i := 0; i < 5; i++ {
		if m.TryAcquireSlot(1) {
			t.Fatal("用户 1 超过单用户上限仍被放行")
		}
	}

	if got := m.curConns.Load(); got != 1 {
		t.Fatalf("全局占用 = %d,应该是 1 —— 被单用户上限拒绝的请求泄漏了全局额度", got)
	}

	m.ReleaseSlot(1)
	if got := m.curConns.Load(); got != 0 {
		t.Fatalf("全部释放后全局占用 = %d,应该是 0", got)
	}
}

// 用户归零后必须从 map 里删掉,否则这个 map 会随历史用户数无限增长。
func TestManagerReleasesUserMapEntry(t *testing.T) {
	m := newManager()
	m.maxConnsPerUser = 2

	for i := int64(0); i < 50; i++ {
		m.TryAcquireSlot(i)
		m.ReleaseSlot(i)
	}

	if got := len(m.userConns); got != 0 {
		t.Fatalf("userConns 残留 %d 个条目,应该是 0 —— 归零时没有 delete(key)", got)
	}
}

// 上限设为 0 = 不限,不能因为没配上限就把所有人拒之门外。
func TestManagerZeroMeansUnlimited(t *testing.T) {
	m := newManager() // maxConns 和 maxConnsPerUser 都是零值

	for i := 0; i < 200; i++ {
		if !m.TryAcquireSlot(1) { // 故意全用同一个 userID
			t.Fatalf("上限为 0(不限)时第 %d 条连接被拒", i+1)
		}
	}
	for i := 0; i < 200; i++ {
		m.ReleaseSlot(1)
	}

	// 不限时根本不该往 userConns 里写东西
	if got := len(m.userConns); got != 0 {
		t.Fatalf("不限模式下 userConns 有 %d 个条目,白白占内存", got)
	}
}

// 并发申请不能突破上限 —— 这是 CAS 循环存在的唯一理由。
// 用 -race 跑。
func TestManagerGlobalLimitIsRaceFree(t *testing.T) {
	m := newManager()
	const limit = 50
	m.maxConns = limit

	const goroutines = 500
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
	)

	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start // 尽量让所有 goroutine 同时冲进来
			if m.TryAcquireSlot(id) {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}(int64(i))
	}
	close(start)
	wg.Wait()

	if accepted != limit {
		t.Fatalf("并发申请放行了 %d 条,上限是 %d —— CAS 循环没挡住超发", accepted, limit)
	}
	if got := m.curConns.Load(); got != int64(limit) {
		t.Fatalf("curConns = %d,应该正好等于放行数 %d", got, limit)
	}

	for i := 0; i < accepted; i++ {
		m.ReleaseSlot(int64(i))
	}
}
