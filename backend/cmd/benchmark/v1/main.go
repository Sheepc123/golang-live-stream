// 压测客户端 v1(已被上一级目录的 v2 取代)。
//
// 保留它只为一件事:docs/benchmark.md 里的历史数据都是它测出来的,
// 要复现那些数字必须用同一个工具。新的压测请用 go run ./cmd/benchmark。
//
// 用法(在 backend/ 下):
//
//	go run ./cmd/benchmark -jwt-secret $JWT_SECRET -conns 1000 -rooms 10 -msg-rate 0.5 -duration 60s
//
// 流程:
//  1. 用 admin 账号登录(不存在就注册),创建 N 个房间并开播
//  2. 用 -jwt-secret 本地签发 conns 个不同 user_id 的 token(不注册,WS 握手只校验 JWT)
//  3. 按 -ramp 的速率阶梯建连,连接按 i % rooms 分配到房间
//  4. 全部建连完成后进入发送阶段:每个连接按 -msg-rate 发 chat,content 里带发送时刻(纳秒)
//  5. 收到 chat 时 now - 发送时刻 = 端到端延迟(同一台机器时钟一致,跨机器要先对时)
//  6. 结束时输出建连成功率、上行 QPS、下行接收数(扇出计数)、延迟分位、丢失率
//
// -msg-rate 0 = 只建连不发消息,用来量「N 个空闲连接占多少内存」。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"github.com/gorilla/websocket"
)

// ---------- 参数 ----------

var (
	flagTarget    = flag.String("target", "http://127.0.0.1:8080", "服务端 HTTP 地址,WS 地址由它推导")
	flagConns     = flag.Int("conns", 1000, "总连接数")
	flagRooms     = flag.Int("rooms", 10, "房间数,连接按 i %% rooms 均分")
	flagRoomIDs   = flag.String("room-ids", "", "复用已有房间(逗号分隔的 id),设了就不再创建房间;房间必须属于 admin")
	flagMsgRate   = flag.Float64("msg-rate", 0.5, "每连接每秒发送弹幕条数,0 = 只建连不发;服务端限流 2/s")
	flagDuration  = flag.Duration("duration", 60*time.Second, "发送阶段持续时间(不含建连时间)")
	flagRamp      = flag.Int("ramp", 200, "每秒建立多少连接")
	flagDrain     = flag.Duration("drain", 3*time.Second, "停止发送后再等多久收尾,避免把在途消息算成丢失")
	flagReport    = flag.Duration("report", 5*time.Second, "运行中每隔多久打印一次进度")
	flagJWTSecret = flag.String("jwt-secret", "", "和服务端相同的 JWT_SECRET,用来本地签发压测用户的 token")
	flagAdminUser = flag.String("admin-user", "bench_admin", "房主账号(用来开播),不存在会自动注册")
	flagAdminPass = flag.String("admin-pass", "bench123456", "房主密码")
	flagUserBase  = flag.Int64("user-base", 1_000_000, "压测用户 user_id 的起始值,避开真实用户")

	keyChat    = []byte(`"type":"chat"`)
	keyError   = []byte(`"type":"error"`)
	keyContent = []byte(`"content":"` + contentPrefix)
)

// ---------- 统计 ----------

// 全部用 atomic:几万个 goroutine 同时更新,加锁会让压测客户端自己先成为瓶颈。
type stats struct {
	connectOK   atomic.Int64 // 握手成功
	connectFail atomic.Int64 // 握手失败(含 503 限额、超时)
	alive       atomic.Int64 // 当前存活连接
	disconnects atomic.Int64 // 建连成功后又被断开(非我们主动关闭)

	sent    atomic.Int64 // 成功写出的 chat
	sendErr atomic.Int64 // 写失败

	recvChat  atomic.Int64 // 收到的 chat(带我们的时间戳前缀)
	recvErr   atomic.Int64 // 收到 type=error(基本就是被限流)
	recvOther atomic.Int64 // like_count / online_count / room_event 等

	lat histogram
}

// histogram 用 1ms 一格的线性桶存延迟,0~10s,最后一格是溢出桶。
// 不存原始样本:6 万连接 × 60 秒 × 每秒几十条,样本会有几千万个,
// 存下来光内存就几百 MB,而且分位数只要 1ms 精度就够用。
const histBuckets = 10001

type histogram struct {
	b     [histBuckets]atomic.Int64
	count atomic.Int64
	max   atomic.Int64
}

func (h *histogram) observe(ms int64) {
	if ms < 0 {
		ms = 0 // 跨机器时钟不同步可能算出负数,归零并在报告里提醒
	}
	if ms >= histBuckets-1 {
		ms = histBuckets - 1
	}
	h.b[ms].Add(1)
	h.count.Add(1)
	for {
		cur := h.max.Load()
		if ms <= cur || h.max.CompareAndSwap(cur, ms) {
			break
		}
	}
}

func (h *histogram) percentile(p float64) int64 {
	total := h.count.Load()
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(total)))
	var acc int64
	for i := range h.b {
		acc += h.b[i].Load()
		if acc >= target {
			return int64(i)
		}
	}
	return histBuckets - 1
}

// ---------- HTTP 辅助 ----------

// 服务端统一响应壳:{code, data, msg}。
type apiResponse struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func apiCall(hc *http.Client, method, rawURL, token string, body any, out any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, rawURL, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var env apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return fmt.Errorf("%s %s: http %d, 响应不是 JSON: %w", method, rawURL, resp.StatusCode, err)
	}
	if env.Code != 0 {
		return fmt.Errorf("%s %s: http %d, code=%d msg=%s", method, rawURL, resp.StatusCode, env.Code, env.Msg)
	}
	if out != nil {
		return json.Unmarshal(env.Data, out)
	}
	return nil
}

// login 登录拿 access_token;账号不存在就先注册再登录。
func login(hc *http.Client, base, user, pass string) (string, error) {
	var out struct {
		AccessToken string `json:"access_token"`
	}
	cred := map[string]string{"username": user, "password": pass}

	err := apiCall(hc, http.MethodPost, base+"/api/v1/auth/login", "", cred, &out)
	if err == nil {
		return out.AccessToken, nil
	}
	fmt.Printf("登录失败(%v),尝试注册 %s\n", err, user)
	if err := apiCall(hc, http.MethodPost, base+"/api/v1/auth/register", "", cred, nil); err != nil {
		return "", fmt.Errorf("注册失败: %w", err)
	}
	if err := apiCall(hc, http.MethodPost, base+"/api/v1/auth/login", "", cred, &out); err != nil {
		return "", fmt.Errorf("注册后登录仍失败: %w", err)
	}
	return out.AccessToken, nil
}

// prepareRooms 创建(或复用)房间并开播。开播接口是幂等的:
// 房间已经在播会直接返回当前场次,所以复用房间反复跑不会报错。
func prepareRooms(hc *http.Client, base, token string, n int, reuse string) ([]int64, error) {
	var ids []int64
	if reuse != "" {
		for _, s := range strings.Split(reuse, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("room-ids 里 %q 不是整数", s)
			}
			ids = append(ids, id)
		}
	} else {
		for i := 0; i < n; i++ {
			var room struct {
				ID int64 `json:"id"`
			}
			body := map[string]string{
				"title":       fmt.Sprintf("bench room %d %s", i, time.Now().Format("0102-150405")),
				"anchor_name": "bench",
				"category":    "bench",
			}
			if err := apiCall(hc, http.MethodPost, base+"/api/v1/rooms", token, body, &room); err != nil {
				return nil, fmt.Errorf("创建房间失败: %w", err)
			}
			ids = append(ids, room.ID)
		}
	}
	for _, id := range ids {
		u := fmt.Sprintf("%s/api/v1/rooms/%d/live/start", base, id)
		if err := apiCall(hc, http.MethodPost, u, token, nil, nil); err != nil {
			return nil, fmt.Errorf("房间 %d 开播失败: %w", id, err)
		}
	}
	return ids, nil
}

// ---------- 连接 ----------

type benchConn struct {
	idx  int
	room int64
	c    *websocket.Conn
}

// 下行消息只解析这两个字段。完整的 Message 结构有十来个字段,
// 全部解析会让客户端在 6 万连接下自己先把 CPU 吃光。
type downMsg struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

const contentPrefix = "b|"

// reader 是每条连接的读循环。它必须一直跑:
// gorilla 的默认 ping handler 是在 ReadMessage 里被触发的,
// 不读就不会回 pong,10 秒后服务端会把这条连接踢掉。
func reader(bc *benchConn, st *stats, closing *atomic.Bool) {
	defer func() {
		st.alive.Add(-1)
		if !closing.Load() {
			st.disconnects.Add(1)
		}
	}()
	for {
		_, frame, err := bc.c.ReadMessage()
		if err != nil {
			return
		}

		for len(frame) > 0 {
			var data []byte
			if i := bytes.IndexByte(frame, '\n'); i >= 0 {
				data, frame = frame[:i], frame[i+1:]
			} else {
				data, frame = frame, nil
			}
			if len(data) == 0 {
				continue
			}
			switch {
			case bytes.Contains(data, keyChat):
				// 在原始字节里找 "content":"b| 这段,后面紧跟的数字就是发送时刻
				i := bytes.Index(data, keyContent)
				if i < 0 {
					st.recvOther.Add(1) // 真人发的弹幕,不是压测流量
					continue
				}
				rest := data[i+len(keyContent):]
				end := bytes.IndexByte(rest, '"')
				if end < 0 {
					continue
				}
				sentAt, err := strconv.ParseInt(string(rest[:end]), 10, 64)
				if err != nil {
					continue
				}
				st.lat.observe((time.Now().UnixNano() - sentAt) / int64(time.Millisecond))
				st.recvChat.Add(1)
			case bytes.Contains(data, keyError):
				st.recvErr.Add(1)
			default:
				st.recvOther.Add(1)
			}

		}
	}
}

// sender 按固定间隔发 chat,直到 ctx 结束。
// 起始时随机偏移一段,否则所有连接会在同一毫秒齐射,
// 测出来的是「同步风暴」而不是稳态负载。
func sender(ctx context.Context, bc *benchConn, rate float64, st *stats, sentPerRoom map[int64]*atomic.Int64) {
	interval := time.Duration(float64(time.Second) / rate)
	jitter := time.Duration(rand.Float64() * float64(interval))
	timer := time.NewTimer(jitter)
	defer timer.Stop()

	roomCounter := sentPerRoom[bc.room]
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		payload := fmt.Sprintf(`{"type":"chat","content":"%s%d"}`, contentPrefix, time.Now().UnixNano())
		_ = bc.c.SetWriteDeadline(time.Now().Add(3 * time.Second))
		if err := bc.c.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
			st.sendErr.Add(1)
			return
		}
		st.sent.Add(1)
		roomCounter.Add(1)
		timer.Reset(interval)
	}
}

// ---------- 主流程 ----------

func main() {
	flag.Parse()
	if *flagJWTSecret == "" {
		fmt.Fprintln(os.Stderr, "必须传 -jwt-secret(和服务端 .env 里的 JWT_SECRET 一致)")
		os.Exit(2)
	}
	if *flagMsgRate > 2 {
		fmt.Println("⚠️  -msg-rate 超过 2,服务端限流是每秒 2 条,超出部分会被拒并计入 error 消息")
	}

	base := strings.TrimRight(*flagTarget, "/")
	wsBase, err := url.Parse(base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "target 不合法:", err)
		os.Exit(2)
	}
	switch wsBase.Scheme {
	case "http":
		wsBase.Scheme = "ws"
	case "https":
		wsBase.Scheme = "wss"
	}

	// Ctrl+C 时停止并打印已有的统计,而不是直接退出什么都不留。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hc := &http.Client{Timeout: 10 * time.Second}

	// 1. 登录 + 房间 + 开播
	adminToken, err := login(hc, base, *flagAdminUser, *flagAdminPass)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rooms, err := prepareRooms(hc, base, adminToken, *flagRooms, *flagRoomIDs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("房间就绪: %v\n", rooms)

	// 2. 本地签发压测用户的 token
	tokens := make([]string, *flagConns)
	for i := range tokens {
		uid := *flagUserBase + int64(i)
		tokens[i], err = Jwttoken.GenerateAccessToken(uid, "bench_"+strconv.Itoa(i), *flagJWTSecret, 24*time.Hour)
		if err != nil {
			fmt.Fprintln(os.Stderr, "签发 token 失败:", err)
			os.Exit(1)
		}
	}

	st := &stats{}
	var closing atomic.Bool
	conns := make([]*benchConn, *flagConns) // 每个 goroutine 只写自己的下标,不需要锁

	// 轮换本地源 IP,绕过单个 IP 约 2.8 万临时端口的上限。
	// loopback 是整个 127.0.0.0/8,127.0.0.2 ~ 127.0.0.N 无需配置即可绑定。
	// 真机压测时把这里换成压测机的多个网卡 IP。

	const localIPs = 4
	var dialIdx atomic.Int64

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
		Proxy:            nil,
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			n := dialIdx.Add(1) % localIPs
			d := &net.Dialer{
				Timeout:   10 * time.Second,
				LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, byte(1+n))},
			}
			return d.DialContext(ctx, network, addr)
		},
	}

	// 3. 阶梯建连
	fmt.Printf("开始建连: %d 条, %d/s\n", *flagConns, *flagRamp)
	rampStart := time.Now()
	go progress(ctx, st, *flagReport)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 256) // 同时进行中的握手数上限
	ticker := time.NewTicker(time.Second / time.Duration(max(*flagRamp, 1)))
dial:
	for i := 0; i < *flagConns; i++ {
		select {
		case <-ctx.Done():
			break dial
		case <-ticker.C:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			room := rooms[i%len(rooms)]
			u := fmt.Sprintf("%s/api/v1/ws/rooms/%d?token=%s", wsBase.String(), room, tokens[i])
			c, resp, err := dialer.DialContext(ctx, u, nil)
			if err != nil {
				st.connectFail.Add(1)
				// 只打印前几个失败原因,否则 1 万个失败会刷屏
				if st.connectFail.Load() <= 5 {
					code := 0
					if resp != nil {
						code = resp.StatusCode
					}
					fmt.Printf("  建连失败 #%d: http=%d err=%v\n", i, code, err)
				}
				return
			}
			bc := &benchConn{idx: i, room: room, c: c}
			conns[i] = bc
			st.connectOK.Add(1)
			st.alive.Add(1)
			go reader(bc, st, &closing)
		}(i)
	}
	ticker.Stop()
	wg.Wait()
	rampCost := time.Since(rampStart)
	fmt.Printf("建连完成: 成功 %d 失败 %d 耗时 %s\n", st.connectOK.Load(), st.connectFail.Load(), rampCost.Round(time.Millisecond))

	// 4. 发送阶段
	//
	// 期望接收数 = Σ(房间内发送数 × 房间内连接数)。
	// 连接数在发送阶段开始时快照一次:中途掉线的连接收不到后续消息,
	// 这部分会体现为「丢失」—— 这是符合用户视角的,掉线的用户确实没收到。
	connsPerRoom := make(map[int64]int64)
	sentPerRoom := make(map[int64]*atomic.Int64)
	for _, r := range rooms {
		sentPerRoom[r] = &atomic.Int64{}
	}
	for _, bc := range conns {
		if bc != nil {
			connsPerRoom[bc.room]++
		}
	}

	sendStart := time.Now()
	if *flagMsgRate > 0 && ctx.Err() == nil {
		fmt.Printf("开始发送: 每连接 %.2f msg/s, 持续 %s\n", *flagMsgRate, *flagDuration)
		sendCtx, cancelSend := context.WithTimeout(ctx, *flagDuration)
		var swg sync.WaitGroup
		for _, bc := range conns {
			if bc == nil {
				continue
			}
			swg.Add(1)
			go func(bc *benchConn) {
				defer swg.Done()
				sender(sendCtx, bc, *flagMsgRate, st, sentPerRoom)
			}(bc)
		}
		<-sendCtx.Done()
		cancelSend()
		swg.Wait()
	} else if ctx.Err() == nil {
		fmt.Printf("只建连不发送,保持 %s(现在去抓 pprof heap / 看 RSS)\n", *flagDuration)
		select {
		case <-ctx.Done():
		case <-time.After(*flagDuration):
		}
	}
	sendCost := time.Since(sendStart)

	// 5. 收尾:等在途消息到齐,再关闭
	if ctx.Err() == nil {
		time.Sleep(*flagDrain)
	}
	closing.Store(true)
	for _, bc := range conns {
		if bc != nil {
			_ = bc.c.Close()
		}
	}

	// 6. 报告
	var expected int64
	for r, n := range connsPerRoom {
		expected += sentPerRoom[r].Load() * n
	}
	report(st, *flagConns, rampCost, sendCost, expected)
}

func progress(ctx context.Context, st *stats, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var lastSent, lastRecv int64
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sent, recv := st.sent.Load(), st.recvChat.Load()
		fmt.Printf("[%4.0fs] 存活 %d | 上行 %d/s | 下行 %d/s | 限流 %d | 掉线 %d\n",
			time.Since(start).Seconds(), st.alive.Load(),
			(sent-lastSent)/int64(every.Seconds()), (recv-lastRecv)/int64(every.Seconds()),
			st.recvErr.Load(), st.disconnects.Load())
		lastSent, lastRecv = sent, recv
	}
}

func report(st *stats, attempted int, rampCost, sendCost time.Duration, expected int64) {
	ok, fail := st.connectOK.Load(), st.connectFail.Load()
	sent, recv := st.sent.Load(), st.recvChat.Load()
	secs := sendCost.Seconds()

	loss := 0.0
	if expected > 0 {
		loss = (1 - float64(recv)/float64(expected)) * 100
	}

	fmt.Println()
	fmt.Println("================ 结果 ================")
	fmt.Printf("连接   尝试 %d | 成功 %d (%.1f%%) | 失败 %d | 运行中掉线 %d | 建连耗时 %s\n",
		attempted, ok, pct(ok, int64(attempted)), fail, st.disconnects.Load(), rampCost.Round(time.Millisecond))
	fmt.Printf("上行   发送 %d 条 | 失败 %d | 平均 %.0f msg/s\n", sent, st.sendErr.Load(), float64(sent)/secs)
	fmt.Printf("下行   收到 %d 条 | 期望 %d | 丢失 %.2f%% | 平均 %.0f msg/s (扇出计数,不是独立请求数)\n",
		recv, expected, loss, float64(recv)/secs)
	fmt.Printf("延迟   p50 %dms | p95 %dms | p99 %dms | max %dms | 样本 %d\n",
		st.lat.percentile(0.50), st.lat.percentile(0.95), st.lat.percentile(0.99), st.lat.max.Load(), st.lat.count.Load())
	fmt.Printf("其他   限流 error %d | 计数/进出场等 %d\n", st.recvErr.Load(), st.recvOther.Load())
	if st.recvErr.Load() > 0 {
		fmt.Println("⚠️  有限流 error:实际上行速率被服务端压低了,上行 QPS 不能按发送数算")
	}
	if errors.Is(context.Canceled, context.Canceled) && st.lat.count.Load() > 0 && st.lat.percentile(0.5) == 0 {
		fmt.Println("ℹ️  p50 为 0ms:同机压测延迟本来就低于 1ms,或跨机器时钟不同步,请核对")
	}
}

func pct(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b) * 100
}
