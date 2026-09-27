package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"github.com/gorilla/websocket"
)

// ---------- 连接分组 ----------
//
// 丢失率只对「发送开始时已经在线」的连接有意义:期望接收数 = 房间发送数 × 房间人数,
// 人数必须在发送开始那一刻快照。发送期间才连上的连接(进场风暴)没法定义期望值,
// 单独成组,只统计延迟。慢客户端也单独成组 —— 隔离测试要比较的正是
// 「慢组很惨的时候,正常组有没有被拖累」。
const (
	grpNormal = iota // 发送开始时在线,正常读取
	grpSlow          // 发送开始时在线,每读一帧故意睡 -slow-delay
	grpLate          // 发送开始之后才连上
	numGroups
)

var groupNames = [numGroups]string{"normal", "slow", "late"}

// 压测消息的 content:b|<计划发送时刻 ns>|<实际发出时刻 ns>
//
// 两个时刻都带上,接收端就能同时算出两种延迟:
//
//	e2e     = 收到 - 计划时刻  → 包含压测端自身的发送滞后(纠正协调遗漏,报告以它为准)
//	service = 收到 - 实际发出  → 只含服务端 + 网络 + 接收端
//
// 两者的差距就是压测端自己拖了多少后腿。
const contentPrefix = "b|"

var (
	keyChat    = []byte(`"type":"chat"`)
	keyError   = []byte(`"type":"error"`)
	keyContent = []byte(`"content":"` + contentPrefix)
)

type benchConn struct {
	idx   int
	room  int64
	group int
	c     *websocket.Conn
	dead  atomic.Bool // 读循环退出后置位;快照时跳过已经断开的连接
}

type groupStats struct {
	conns   atomic.Int64
	recv    atomic.Int64
	e2e     hist
	service hist
}

// tlSlot 是时间线上的一秒。延迟按「收到的那一秒」归档,
// 这样进场风暴、故障注入发生的那几秒能直接�� CSV 里看出来。
type tlSlot struct {
	alive, sent, recv, skipped, sendErr, disconnects atomic.Int64
	lat                                              hist
}

// runState 是一轮压测的全部计数。几万个 goroutine 同时更新,
// 所以热路径上全部用 atomic —— 加锁会让压测端自己先成为瓶颈。
type runState struct {
	cfg       Config
	runStart  time.Time
	sendStart atomic.Int64 // UnixNano,0 = 还没开始发送
	closing   atomic.Bool  // 收尾阶段主动关连接,不计入掉线

	connectOK, connectFail, alive, disconnects atomic.Int64

	senders, planned, sent, skipped, sendErr atomic.Int64
	senderLag                                hist // 实际发出 - 计划时刻

	groups             [numGroups]groupStats
	recvErr, recvOther atomic.Int64

	timeline []tlSlot

	// mu 只保护下面这几个冷路径字段(失败原因、告警、profile 路径、故障事件)
	mu          sync.Mutex
	failReasons map[string]int64
	warnings    []string
	profiles    []string
	faults      []FaultEvent
}

func newRunState(cfg Config) *runState {
	dialConns := cfg.Conns
	if cfg.Warm > 0 {
		dialConns = cfg.Warm
	}
	// 时间线长度按「建连 + 发送 + 收尾」估算,再留 2 分钟余量(风暴建连、握手超时)。
	// 超出的部分会归进最后一格,不会越界。
	secs := dialConns/cfg.Ramp + int((cfg.Duration+cfg.Drain)/time.Second) + 120
	return &runState{
		cfg:         cfg,
		failReasons: map[string]int64{},
		timeline:    make([]tlSlot, secs),
	}
}

func (s *runState) slot(t time.Time) *tlSlot {
	i := int(t.Sub(s.runStart) / time.Second)
	return &s.timeline[max(0, min(i, len(s.timeline)-1))]
}

func (s *runState) warnf(format string, args ...any) {
	s.mu.Lock()
	s.warnings = append(s.warnings, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

func (s *runState) totalRecv() int64 {
	var n int64
	for g := range numGroups {
		n += s.groups[g].recv.Load()
	}
	return n
}

// ---------- 建连 ----------

// dialPlan 是一轮里所有连接的「剧本」:谁进哪个房间、谁发言、谁是慢客户端。
type dialPlan struct {
	wsBase  string
	rooms   []int64
	tokens  []string
	dialer  *websocket.Dialer
	conns   []*benchConn // 每个建连 goroutine 只写自己的下标,不需要锁
	speaker []bool
	slow    []bool
}

func newDialPlan(cfg Config, rooms []int64, wsBase string) (*dialPlan, error) {
	p := &dialPlan{
		wsBase:  wsBase,
		rooms:   rooms,
		tokens:  make([]string, cfg.Conns),
		dialer:  newDialer(cfg.LocalIPs),
		conns:   make([]*benchConn, cfg.Conns),
		speaker: make([]bool, cfg.Conns),
		slow:    make([]bool, cfg.Conns),
	}
	// 用固定种子抽签:同一个 -seed 每一轮挑中的是同一批发言者和慢客户端,
	// 多轮之间的差异才只来自系统本身,而不是抽签运气。
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	for i := range cfg.Conns {
		p.speaker[i] = rng.Float64() < cfg.Speakers
		p.slow[i] = rng.Float64() < cfg.SlowFrac

		// 本地签发 token,不走注册:WS 握手只校验 JWT
		uid := cfg.UserBase + int64(i)
		tok, err := Jwttoken.GenerateAccessToken(uid, "bench_"+strconv.Itoa(i), cfg.JWTSecret, 24*time.Hour)
		if err != nil {
			return nil, fmt.Errorf("签发 token 失败: %w", err)
		}
		p.tokens[i] = tok
	}
	return p, nil
}

// newDialer 轮换本地源 IP,绕开单个 IP 约 2.8 万临时端口的上限。
// loopback 是整个 127.0.0.0/8,127.0.0.2 ~ 127.0.0.N 不用配置就能绑定;
// 目标不是本机时 main 会把 localIPs 设成 1,由系统自己选源地址。
func newDialer(localIPs int) *websocket.Dialer {
	var next atomic.Int64
	return &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{Timeout: 10 * time.Second}
			if localIPs > 1 {
				n := next.Add(1) % int64(localIPs)
				d.LocalAddr = &net.TCPAddr{IP: net.IPv4(127, 0, 0, byte(1+n))}
			}
			return d.DialContext(ctx, network, addr)
		},
	}
}

// dialRange 以 rate 条/s 的速度建立 [from, to) 这些连接,全部完成(或失败)后返回。
func (s *runState) dialRange(ctx context.Context, p *dialPlan, from, to, rate int, late bool) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 256) // 同时进行中的握手上限,防止瞬间几千个半开连接
	ticker := time.NewTicker(max(time.Second/time.Duration(rate), time.Microsecond))
	defer ticker.Stop()

loop:
	for i := from; i < to; i++ {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}
		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.dialOne(ctx, p, i, late)
		}()
	}
	wg.Wait()
}

func (s *runState) dialOne(ctx context.Context, p *dialPlan, i int, late bool) {
	room := p.rooms[i%len(p.rooms)]
	u := fmt.Sprintf("%s/api/v1/ws/rooms/%d?token=%s", p.wsBase, room, p.tokens[i])
	c, resp, err := p.dialer.DialContext(ctx, u, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		s.connectFail.Add(1)
		reason := failReason(resp, err)
		s.mu.Lock()
		s.failReasons[reason]++
		s.mu.Unlock()
		return
	}

	group := grpNormal
	var slowDelay time.Duration
	switch {
	case late:
		group = grpLate
		s.groups[grpLate].conns.Add(1)
	case p.slow[i]:
		group = grpSlow
		slowDelay = s.cfg.SlowDelay
	}
	bc := &benchConn{idx: i, room: room, group: group, c: c}
	p.conns[i] = bc
	s.connectOK.Add(1)
	s.alive.Add(1)
	go s.reader(bc, slowDelay)
}

// failReason 把错误归类成「原因」:去掉地址、端口这些每次都不一样的前缀,
// 否则 900 个同样原因的失败会变成 900 个不同的字符串。
func failReason(resp *http.Response, err error) string {
	if resp != nil {
		return fmt.Sprintf("http %d", resp.StatusCode)
	}
	msg := err.Error()
	if i := strings.LastIndex(msg, ": "); i >= 0 {
		msg = msg[i+2:]
	}
	return msg
}

// ---------- 读 ----------

// reader 是每条连接的读循环。它必须一直跑:gorilla 的默认 ping handler
// 是在 ReadMessage 里触发的,不读就不回 pong,服务端 10 秒后会踢掉这条连接。
func (s *runState) reader(bc *benchConn, slowDelay time.Duration) {
	defer func() {
		bc.dead.Store(true)
		s.alive.Add(-1)
		if !s.closing.Load() {
			s.disconnects.Add(1)
			s.slot(time.Now()).disconnects.Add(1)
		}
	}()

	g := &s.groups[bc.group]
	runStartNano := s.runStart.UnixNano()
	for {
		_, frame, err := bc.c.ReadMessage()
		if err != nil {
			return
		}
		now := time.Now()
		nowNano := now.UnixNano()
		slot := s.slot(now)

		// 服务端合并写:一帧里可能有多条,用 '\n' 分隔(NDJSON)。
		// 只在原始字节里找需要的字段,不做完整 JSON 解析 ——
		// 几千条连接一起解析会让压测端自己先把 CPU 吃光。
		for len(frame) > 0 {
			var line []byte
			if i := bytes.IndexByte(frame, '\n'); i >= 0 {
				line, frame = frame[:i], frame[i+1:]
			} else {
				line, frame = frame, nil
			}
			if len(line) == 0 {
				continue
			}
			switch {
			case bytes.Contains(line, keyChat):
				intended, actual, ok := parseBenchContent(line)
				if !ok || intended < runStartNano {
					s.recvOther.Add(1) // 真人弹幕,或上一轮残留的在途消息
					continue
				}
				e2e := (nowNano - intended) / 1e3
				g.recv.Add(1)
				g.e2e.observe(e2e)
				g.service.observe((nowNano - actual) / 1e3)
				slot.recv.Add(1)
				slot.lat.observe(e2e)
			case bytes.Contains(line, keyError):
				s.recvErr.Add(1) // 基本就是被限流,或房间没开播
			default:
				s.recvOther.Add(1) // like_count / online_count / room_event
			}
		}

		if slowDelay > 0 {
			// 慢客户端:读得慢 → 本机接收缓冲满 → 服务端 socket 发送缓冲满
			// → WritePump 的 write 阻塞 → Send 通道满 → 服务端开始 client_slow 丢弃。
			// 隔离做得好的话,这一切都只该发生在这条连接上。
			time.Sleep(slowDelay)
		}
	}
}

// parseBenchContent 从一条 chat 的原始 JSON 里取出 b|<计划>|<实际> 两个时间戳。
func parseBenchContent(line []byte) (intended, actual int64, ok bool) {
	i := bytes.Index(line, keyContent)
	if i < 0 {
		return 0, 0, false
	}
	rest := line[i+len(keyContent):]
	intended, rest, ok = parseDigits(rest, '|')
	if !ok {
		return 0, 0, false
	}
	actual, _, ok = parseDigits(rest, '"')
	return intended, actual, ok
}

// parseDigits 读一串十进制数字,要求紧跟着 end 字符。手写是为了零分配。
func parseDigits(b []byte, end byte) (int64, []byte, bool) {
	var n int64
	i := 0
	for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		n = n*10 + int64(b[i]-'0')
	}
	if i == 0 || i >= len(b) || b[i] != end {
		return 0, nil, false
	}
	return n, b[i+1:], true
}

// ---------- 写 ----------

// sender 以开环方式发送:第 i 条的计划时刻 = first + i*interval,
// 和前一条什么时候发完无关。
//
// ── 为什么这样能纠正协调遗漏(coordinated omission)──
// v1 是「发完一条再 Reset(interval)」。压测端一旦被拖慢就悄悄少发,
// 而那些「本该在拥塞时刻发出」的消息根本不产生延迟样本 —— 偏偏是最慢的那批。
// 开环排程下落后了就立刻补发,延迟从计划时刻算起,拥塞的代价被如实记进 e2e。
//
// 落后超过 -max-lag 的不再补(计入 skipped):补发一串会撞上服务端每秒 2 条的限流,
// 而且这时压测端已经饱和,结果本身就要打折扣 —— 报告会给出告警。
func (s *runState) sender(ctx context.Context, bc *benchConn, roomSent *atomic.Int64, first time.Time, n int64, interval time.Duration) {
	buf := make([]byte, 0, 64)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	for i := range n {
		due := first.Add(time.Duration(i) * interval)
		if wait := time.Until(due); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return
		}

		now := time.Now()
		lag := now.Sub(due)
		s.senderLag.observe(lag.Microseconds())
		slot := s.slot(now)
		if lag > s.cfg.MaxLag {
			s.skipped.Add(1)
			slot.skipped.Add(1)
			continue
		}

		buf = fmt.Appendf(buf[:0], `{"type":"chat","content":"%s%d|%d"}`, contentPrefix, due.UnixNano(), now.UnixNano())
		_ = bc.c.SetWriteDeadline(now.Add(3 * time.Second))
		if err := bc.c.WriteMessage(websocket.TextMessage, buf); err != nil {
			s.sendErr.Add(1)
			slot.sendErr.Add(1)
			return
		}
		s.sent.Add(1)
		roomSent.Add(1)
		slot.sent.Add(1)
	}
}

// ---------- 一轮 ----------

func runOnce(ctx context.Context, cfg Config, rooms []int64, run int, wsBase string, obs *observer, outDir string) *RunResult {
	s := newRunState(cfg)
	name := fmt.Sprintf("%s-run%d", cfg.Scenario, run)

	plan, err := newDialPlan(cfg, rooms, wsBase)
	if err != nil {
		s.warnf("%v", err)
		return s.result(runMeta{name: name, run: run, startedAt: time.Now(), aborted: true})
	}

	// 建连前拍一张:只用它的内存和 goroutine 数当基线,算「每连接成本」
	pre := obs.take(ctx)

	s.runStart = time.Now()
	monitorDone := make(chan struct{})
	go s.monitor(monitorDone)
	defer close(monitorDone)

	warm := cfg.Conns
	if cfg.Warm > 0 {
		warm = cfg.Warm
	}
	fmt.Printf("建连: %d 条, %d 条/s\n", warm, cfg.Ramp)
	s.dialRange(ctx, plan, 0, warm, cfg.Ramp, false)
	rampCost := time.Since(s.runStart)
	fmt.Printf("建连完成: 成功 %d 失败 %d 耗时 %s\n", s.connectOK.Load(), s.connectFail.Load(), rampCost.Round(time.Millisecond))

	// ---- 快照:发送开始时谁在场 ----
	// 期望接收数 = Σ(房间发送数 × 房间里该组的人数)。中途掉线的连接收不到后续消息,
	// 会体现为丢失 —— 这符合用户视角,掉线的用户确实没收到。
	perRoom := make(map[int64]*[numGroups]int64, len(rooms))
	roomSent := make(map[int64]*atomic.Int64, len(rooms))
	for _, r := range rooms {
		perRoom[r] = new([numGroups]int64)
		roomSent[r] = &atomic.Int64{}
	}
	for _, bc := range plan.conns[:warm] {
		if bc == nil || bc.dead.Load() {
			continue
		}
		perRoom[bc.room][bc.group]++
		s.groups[bc.group].conns.Add(1)
	}

	before := obs.take(ctx)
	sendStart := time.Now()
	s.sendStart.Store(sendStart.UnixNano())
	deadline := sendStart.Add(cfg.Duration)

	var bg sync.WaitGroup // 故障注入、CPU profile 这些后台任务

	if cfg.FaultCmd != "" {
		bg.Add(1)
		go func() {
			defer bg.Done()
			events := runFaults(ctx, cfg, sendStart)
			s.mu.Lock()
			s.faults = events
			s.mu.Unlock()
		}()
	}

	// CPU profile 放在发送阶段中段:避开开头的爬坡和结尾的收尾
	if secs := min(cfg.ProfileSecs, int(cfg.Duration/2/time.Second)); cfg.PprofURL != "" && secs >= 1 {
		bg.Add(1)
		go func() {
			defer bg.Done()
			if !sleepCtx(ctx, cfg.Duration/4) {
				return
			}
			path := filepath.Join(outDir, name+"-cpu.prof")
			if err := fetchProfile(ctx, cfg.PprofURL, "profile", secs, path); err != nil {
				s.warnf("抓取 CPU profile 失败: %v", err)
				return
			}
			s.mu.Lock()
			s.profiles = append(s.profiles, path)
			s.mu.Unlock()
		}()
	}

	// ---- 发送 ----
	var swg sync.WaitGroup
	if cfg.MsgRate > 0 && ctx.Err() == nil {
		interval := time.Duration(float64(time.Second) / cfg.MsgRate)
		// 每个发言者的起始相位随机:否则所有连接在同一毫秒齐射,
		// 测出来的是「同步风暴」而不是稳态负载
		phaseRng := rand.New(rand.NewPCG(cfg.Seed+1, uint64(run)))
		for _, bc := range plan.conns[:warm] {
			if bc == nil || bc.dead.Load() || !plan.speaker[bc.idx] {
				continue
			}
			first := sendStart.Add(time.Duration(phaseRng.Float64() * float64(interval)))
			if !first.Before(deadline) {
				continue
			}
			n := int64((deadline.Sub(first) + interval - 1) / interval) // 窗口内的计划条数
			s.senders.Add(1)
			s.planned.Add(n)
			swg.Add(1)
			go func() {
				defer swg.Done()
				s.sender(ctx, bc, roomSent[bc.room], first, n, interval)
			}()
		}
		fmt.Printf("开始发送: %d 个发言者 × %.2f msg/s, 计划 %.0f msg/s, 持续 %s\n",
			s.senders.Load(), cfg.MsgRate, float64(s.planned.Load())/cfg.Duration.Seconds(), cfg.Duration)
	} else if ctx.Err() == nil {
		fmt.Printf("只建连不发送, 保持 %s\n", cfg.Duration)
	}

	// ---- 进场风暴 ----
	// 等发送稳定一会儿再涌入,时间线里就能看到「风暴前 / 风暴中 / 风暴后」三段对比
	stormDone := make(chan struct{})
	if warm < cfg.Conns && ctx.Err() == nil {
		go func() {
			defer close(stormDone)
			if !sleepCtx(ctx, min(10*time.Second, cfg.Duration/4)) {
				return
			}
			fmt.Printf("进场风暴: %d 条, %d 条/s\n", cfg.Conns-warm, cfg.StormRamp)
			s.dialRange(ctx, plan, warm, cfg.Conns, cfg.StormRamp, true)
			fmt.Printf("进场风暴结束: 累计成功 %d 失败 %d\n", s.connectOK.Load(), s.connectFail.Load())
		}()
	} else {
		close(stormDone)
	}

	sleepCtx(ctx, time.Until(deadline))
	swg.Wait()
	sendSecs := min(time.Since(sendStart), cfg.Duration).Seconds()
	<-stormDone

	// 收尾:等在途消息到齐。被 Ctrl+C 时直接跳过
	sleepCtx(ctx, cfg.Drain)

	// heap 要在关连接之前抓,反映的是「满载在线」时的内存
	if cfg.PprofURL != "" {
		path := filepath.Join(outDir, name+"-heap.prof")
		if err := fetchProfile(context.Background(), cfg.PprofURL, "heap", 0, path); err != nil {
			s.warnf("抓取 heap profile 失败: %v", err)
		} else {
			s.mu.Lock()
			s.profiles = append(s.profiles, path)
			s.mu.Unlock()
		}
	}

	// 用 Background:被 Ctrl+C 时也要拿到结束快照,才能出一份部分报告
	after := obs.take(context.Background())
	aliveAtEnd := s.alive.Load()
	bg.Wait()

	s.closing.Store(true)
	for _, bc := range plan.conns {
		if bc != nil {
			_ = bc.c.Close()
		}
	}
	waitFor(5*time.Second, func() bool { return s.alive.Load() == 0 })

	return s.result(runMeta{
		name:       name,
		run:        run,
		startedAt:  s.runStart,
		rampCost:   rampCost,
		sendSecs:   sendSecs,
		aborted:    ctx.Err() != nil,
		perRoom:    perRoom,
		roomSent:   roomSent,
		aliveAtEnd: aliveAtEnd,
		pre:        pre,
		before:     before,
		after:      after,
	})
}

// monitor 每秒记录一次存活连接数,每隔 -report 打印一行进度。
func (s *runState) monitor(done <-chan struct{}) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastPrint := time.Now()
	var lastSent, lastRecv int64
	for {
		select {
		case <-done:
			return
		case now := <-tick.C:
			s.slot(now).alive.Store(s.alive.Load())
			if now.Sub(lastPrint) < s.cfg.Report {
				continue
			}
			secs := now.Sub(lastPrint).Seconds()
			sent, recv := s.sent.Load(), s.totalRecv()
			fmt.Printf("  [%4.0fs] 存活 %d | 上行 %.0f/s | 下行 %.0f/s | 滞后跳过 %d | error 消息 %d | 掉线 %d\n",
				now.Sub(s.runStart).Seconds(), s.alive.Load(),
				float64(sent-lastSent)/secs, float64(recv-lastRecv)/secs,
				s.skipped.Load(), s.recvErr.Load(), s.disconnects.Load())
			lastSent, lastRecv, lastPrint = sent, recv, now
		}
	}
}

func waitFor(timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
}
