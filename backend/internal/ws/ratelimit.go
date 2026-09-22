package ws

import "time"

// tokenBucket 是一个不加锁的令牌桶,用来限制单条连接的上行速率。
//
// ── 为什么不用 golang.org/x/time/rate ──
//
// 标准做法是 rate.NewLimiter(2, 5)。但它内部有一把 sync.Mutex,
// 而这个桶只会被一个 goroutine 碰 —— 每条连接的 ReadPump 是它自己
// limiter 的唯一调用者(ReadPump → Dispatch → Action.Execute 全程
// 在同一个栈上同步执行,没有任何地方把 Client 交给别的 goroutine)。
//
// 「单 goroutine 独占」这个约束让那把锁变成纯开销:
//
//	rate.Limiter  ≈ 80 字节 + 每条消息一次 Lock/Unlock
//	tokenBucket   = 32 字节 + 几次浮点运算
//
// 6 万连接 × 每连接 2 个桶,这是 ~9 MB 和 ~3.8 MB 的差别。
// 在「单连接 < 1 KB」的预算下,这个量级值得自己写。
//
// ⚠️ 代价:它不是并发安全的。哪天有人从别的 goroutine 调 allow(),
// 就是一个 data race。这条注释和 Client 上的字段注释是唯一的护栏。
//
// ── 令牌桶 vs 漏桶 ──
//
// 漏桶强制匀速流出:每 500ms 只放行一条,多的排队或丢弃。
// 令牌桶允许攒额度,攒到 burst 上限:安静一会儿之后可以连发 5 条。
// 弹幕要的是后者 —— 真人打字本来就是「想半天、连发三条」的节奏,
// 匀速限流会把正常用户误伤得很难受。
type tokenBucket struct {
	// tokens 当前可用令牌数。必须是浮点数 ——
	// 用整数的话,「每秒 2 个」在 300ms 的间隔里补充 0.6 个会被截断成 0,
	// 桶永远攒不满,实际限流速率会远低于设定值。
	tokens float64

	// last 上次补充令牌的时刻。
	last time.Time

	rate  float64 // 每秒补充多少个令牌
	burst float64 // 桶容量,决定能攒多少突发额度
}

func newTokenBucket(ratePerSec, burst float64) tokenBucket {
	return tokenBucket{
		// 初始给满:用户刚连上就该能立刻发言,
		// 而不是先等半秒攒令牌 —— 那是个莫名其妙的首次体验。
		tokens: burst,
		last:   time.Now(),
		rate:   ratePerSec,
		burst:  burst,
	}
}

// allow 取走一个令牌。没有就返回 false —— 不阻塞、不排队、不重试。
//
// 为什么不阻塞:调用方是 ReadPump,阻塞它等于让这条连接停止读取,
// 连心跳 pong 都处理不了,最后被自己的 ReadDeadline 踢掉。
// 限流的正确行为是「立刻拒绝这一条」,不是「让你慢点」。
//
// now 由调用方传入,方便测试注入时间,不用去动系统时钟。
// 只能从拥有这条连接的 ReadPump goroutine 调用。
func (b *tokenBucket) allow(now time.Time) bool {
	// time.Now() 带单调时钟读数,Sub 出来的间隔不会因为 NTP 校时变成负数。
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.last = now
		b.tokens += elapsed * b.rate

		// 必须封顶。不封的话,挂机一小时回来能瞬间发 7200 条 ——
		// 那不是限流,是替刷屏的人攒弹药。
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
