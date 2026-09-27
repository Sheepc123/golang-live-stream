package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// runMeta 是 runOnce 交给 result 的原始材料。
type runMeta struct {
	name       string
	run        int
	startedAt  time.Time
	rampCost   time.Duration
	sendSecs   float64
	aborted    bool
	perRoom    map[int64]*[numGroups]int64 // 发送开始时每个房间各组的人数
	roomSent   map[int64]*atomic.Int64     // 每个房间实际发出的消息数
	aliveAtEnd int64

	pre    *snapshot // 建连之前
	before *snapshot // 发送开始时
	after  *snapshot // 收尾之后
}

// ---------- 结果结构(写进 JSON) ----------

type RunResult struct {
	Name      string            `json:"name"`
	Scenario  string            `json:"scenario"`
	Run       int               `json:"run"`
	StartedAt time.Time         `json:"started_at"`
	Aborted   bool              `json:"aborted"`
	Params    map[string]string `json:"params"`

	Load     LoadResult              `json:"load"`
	Delivery DeliveryResult          `json:"delivery"`
	Groups   map[string]*GroupResult `json:"groups"`
	Server   *ServerResult           `json:"server,omitempty"`
	Consumer *ConsumerResult         `json:"consumer,omitempty"`
	Redis    *RedisResult            `json:"redis,omitempty"`
	Client   *ClientResult           `json:"client,omitempty"`

	Faults   []FaultEvent `json:"faults,omitempty"`
	Profiles []string     `json:"profiles,omitempty"`
	Warnings []string     `json:"warnings,omitempty"`

	Timeline []TimelineRow `json:"-"` // 单独写 CSV
}

type LoadResult struct {
	Attempted    int              `json:"attempted"`
	ConnectOK    int64            `json:"connect_ok"`
	ConnectFail  int64            `json:"connect_fail"`
	FailReasons  map[string]int64 `json:"fail_reasons,omitempty"`
	RampSecs     float64          `json:"ramp_secs"`
	AliveAtStart int64            `json:"alive_at_send_start"`
	AliveAtEnd   int64            `json:"alive_at_end"`
	Disconnects  int64            `json:"disconnects"`

	SendSecs     float64   `json:"send_secs"`
	Senders      int64     `json:"senders"`
	Planned      int64     `json:"planned"`
	Sent         int64     `json:"sent"`
	Skipped      int64     `json:"skipped"`
	SendErr      int64     `json:"send_errors"`
	PlannedRate  float64   `json:"planned_rate"`
	ActualRate   float64   `json:"actual_rate"`
	SendRatioPct float64   `json:"send_ratio_pct"`
	SenderLag    Quantiles `json:"sender_lag"`
}

// DeliveryResult 只统计「发送开始时就在线」的连接(normal + slow 组)。
type DeliveryResult struct {
	Expected  int64    `json:"expected"`
	Received  int64    `json:"received"`
	LossPct   *float64 `json:"loss_pct,omitempty"`
	RecvRate  float64  `json:"recv_rate"` // 所有组收到的 chat / 发送时长,扇出计数
	ErrorMsgs int64    `json:"error_msgs"`
	OtherMsgs int64    `json:"other_msgs"`
}

type GroupResult struct {
	Conns    int64     `json:"conns"`
	Expected int64     `json:"expected,omitempty"`
	Received int64     `json:"received"`
	LossPct  *float64  `json:"loss_pct,omitempty"`
	E2E      Quantiles `json:"e2e"`     // 收到 - 计划时刻
	Service  Quantiles `json:"service"` // 收到 - 实际发出时刻
}

type Segment struct {
	Name    string  `json:"name"`
	Label   string  `json:"label"`
	Present bool    `json:"present"`
	P50ms   float64 `json:"p50_ms"`
	P99ms   float64 `json:"p99_ms"`
}

type ServerResult struct {
	WindowSecs           float64   `json:"window_secs"`
	Cores                float64   `json:"cores"`
	DownTotal            float64   `json:"down_total"`
	DownChat             float64   `json:"down_chat"`
	DownRate             float64   `json:"down_rate"`
	DeliveriesPerCoreSec float64   `json:"deliveries_per_core_sec"`
	ClientVsServerPct    *float64  `json:"client_recv_vs_server_down_pct,omitempty"`
	DroppedClientSlow    float64   `json:"dropped_client_slow"`
	DroppedQueueFullJobs float64   `json:"dropped_queue_full_jobs"`
	DroppedRateLimited   float64   `json:"dropped_rate_limited"`
	Rejected             float64   `json:"rejected"`
	DropPct              float64   `json:"drop_pct"`
	Writes               float64   `json:"writes"`
	WriteBatchAvg        float64   `json:"write_batch_avg"`
	WriteBytesAvg        *float64  `json:"write_bytes_avg,omitempty"`
	Segments             []Segment `json:"segments"`
	KafkaOK              float64   `json:"kafka_ok"`
	KafkaErr             float64   `json:"kafka_err"`
	IdleRSSPerConnKB     *float64  `json:"idle_rss_per_conn_kb,omitempty"`
	IdleHeapPerConnKB    *float64  `json:"idle_heap_per_conn_kb,omitempty"`
	GoroutinesPerConn    *float64  `json:"goroutines_per_conn,omitempty"`
}

type ConsumerResult struct {
	OK           float64 `json:"ok"`
	WriteErr     float64 `json:"write_error"`
	UnmarshalErr float64 `json:"unmarshal_error"`
	Rate         float64 `json:"rate"`
	BatchAvg     float64 `json:"batch_avg"`
	LagP99s      float64 `json:"lag_p99_s"`
	WriteP99ms   float64 `json:"write_p99_ms"`
}

type RedisResult struct {
	CommandsPerSec float64 `json:"commands_per_sec"`
	NetOutMBps     float64 `json:"net_out_mbps"`
	NetInMBps      float64 `json:"net_in_mbps"`
}

type ClientResult struct {
	Cores        float64 `json:"cores"`
	MachineCores int     `json:"machine_cores"`
}

type TimelineRow struct {
	Sec         int
	Phase       string
	Alive       int64
	Sent        int64
	Recv        int64
	Skipped     int64
	SendErr     int64
	Disconnects int64
	Lat         Quantiles
}

// 服务端分段延迟:按消息在链路上的先后顺序排。
var segmentDefs = []struct{ key, metric, label string }{
	{"redis_publish", "live_redis_publish_duration_seconds", "Redis PUBLISH(上行,同步往返)"},
	{"queue_wait", "live_broadcast_queue_wait_seconds", "广播池排队"},
	{"deliver", "live_broadcast_duration_seconds", "deliver(扇出进 Send 通道)"},
	{"write", "live_ws_write_duration_seconds", "WritePump 合并写"},
}

// ---------- 汇总 ----------

func (s *runState) result(m runMeta) *RunResult {
	cfg := s.cfg
	r := &RunResult{
		Name:      m.name,
		Scenario:  cfg.Scenario,
		Run:       m.run,
		StartedAt: m.startedAt,
		Aborted:   m.aborted,
		Groups:    map[string]*GroupResult{},
	}

	aliveAtStart := s.groups[grpNormal].conns.Load() + s.groups[grpSlow].conns.Load()
	planned, sent := s.planned.Load(), s.sent.Load()

	s.mu.Lock()
	reasons := make(map[string]int64, len(s.failReasons))
	for k, v := range s.failReasons {
		reasons[k] = v
	}
	r.Faults = slices.Clone(s.faults)
	r.Profiles = slices.Clone(s.profiles)
	r.Warnings = slices.Clone(s.warnings)
	s.mu.Unlock()

	r.Load = LoadResult{
		Attempted:    cfg.Conns,
		ConnectOK:    s.connectOK.Load(),
		ConnectFail:  s.connectFail.Load(),
		FailReasons:  reasons,
		RampSecs:     round(m.rampCost.Seconds(), 1),
		AliveAtStart: aliveAtStart,
		AliveAtEnd:   m.aliveAtEnd,
		Disconnects:  s.disconnects.Load(),
		SendSecs:     round(m.sendSecs, 1),
		Senders:      s.senders.Load(),
		Planned:      planned,
		Sent:         sent,
		Skipped:      s.skipped.Load(),
		SendErr:      s.sendErr.Load(),
		PlannedRate:  round(float64(planned)/cfg.Duration.Seconds(), 1),
		ActualRate:   round(perSec(float64(sent), m.sendSecs), 1),
		SenderLag:    s.senderLag.summary(),
	}
	if planned > 0 {
		r.Load.SendRatioPct = round(float64(sent)/float64(planned)*100, 2)
	}

	// ---- 分组 ----
	var expected, received int64
	for g := range numGroups {
		gs := &s.groups[g]
		if gs.conns.Load() == 0 {
			continue
		}
		gr := &GroupResult{
			Conns:    gs.conns.Load(),
			Received: gs.recv.Load(),
			E2E:      gs.e2e.summary(),
			Service:  gs.service.summary(),
		}
		if g != grpLate && m.perRoom != nil {
			for room, counts := range m.perRoom {
				gr.Expected += m.roomSent[room].Load() * counts[g]
			}
			gr.LossPct = lossPct(gr.Received, gr.Expected)
			expected += gr.Expected
			received += gr.Received
		}
		r.Groups[groupNames[g]] = gr
	}
	r.Delivery = DeliveryResult{
		Expected:  expected,
		Received:  received,
		LossPct:   lossPct(received, expected),
		RecvRate:  round(perSec(float64(s.totalRecv()), m.sendSecs), 1),
		ErrorMsgs: s.recvErr.Load(),
		OtherMsgs: s.recvOther.Load(),
	}

	// ---- 外部观测 ----
	if m.before != nil && m.after != nil {
		win := m.after.at.Sub(m.before.at).Seconds()
		r.Client = &ClientResult{
			Cores:        round((m.after.clientCPU-m.before.clientCPU).Seconds()/win, 2),
			MachineCores: runtime.NumCPU(),
		}
		switch {
		case m.before.serverErr != nil:
			r.Warnings = append(r.Warnings, fmt.Sprintf("服务端指标不可用: %v", m.before.serverErr))
		case m.after.serverErr != nil:
			r.Warnings = append(r.Warnings, fmt.Sprintf("服务端指标不可用: %v", m.after.serverErr))
		default:
			r.Server = serverResult(m, win, s.totalRecv(), aliveAtStart)
		}
		if m.before.consumer != nil && m.after.consumer != nil {
			r.Consumer = consumerResult(m.after.consumer.sub(m.before.consumer), win)
		}
		if m.before.redis != nil && m.after.redis != nil {
			d := func(k string) float64 { return m.after.redis[k] - m.before.redis[k] }
			r.Redis = &RedisResult{
				CommandsPerSec: round(d("total_commands_processed")/win, 0),
				NetOutMBps:     round(d("total_net_output_bytes")/win/1e6, 2),
				NetInMBps:      round(d("total_net_input_bytes")/win/1e6, 2),
			}
		}
	}

	r.Timeline = s.timelineRows()
	diagnose(r, cfg)
	return r
}

func serverResult(m runMeta, win float64, clientRecv, aliveAtStart int64) *ServerResult {
	d := m.after.server.sub(m.before.server)
	sr := &ServerResult{WindowSecs: round(win, 1)}

	cpu := d.total("process_cpu_seconds_total")
	sr.Cores = round(cpu/win, 2)
	sr.DownTotal = d.total("live_ws_messages_total", "direction", "down")
	sr.DownChat = d.total("live_ws_messages_total", "direction", "down", "type", "chat")
	sr.DownRate = round(sr.DownTotal/win, 0)
	if cpu > 0 {
		// ΔCPU 和 Δ投递数取自同一个时间窗口,时长在除法里约掉了,
		// 所以这个数不受「窗口里有多少空闲时间」影响
		sr.DeliveriesPerCoreSec = round(sr.DownTotal/cpu, 0)
	}
	if sr.DownChat > 0 {
		v := round(float64(clientRecv)/sr.DownChat*100, 2)
		sr.ClientVsServerPct = &v
	}

	sr.DroppedClientSlow = d.total("live_ws_dropped_total", "reason", "client_slow")
	sr.DroppedQueueFullJobs = d.total("live_ws_dropped_total", "reason", "queue_full")
	sr.DroppedRateLimited = d.total("live_ws_dropped_total", "reason", "rate_limited")
	sr.Rejected = d.total("live_ws_rejected_total")
	// queue_full 是按 job 计的(一个 job = 整个房间的一条消息),和逐连接的
	// client_slow 不是一个单位,所以丢弃率只用 client_slow 算
	if tot := sr.DownTotal + sr.DroppedClientSlow; tot > 0 {
		sr.DropPct = round(sr.DroppedClientSlow/tot*100, 3)
	}

	if n := d.total("live_ws_write_batch_size_count"); n > 0 {
		sr.Writes = n
		sr.WriteBatchAvg = round(d.total("live_ws_write_batch_size_sum")/n, 2)
		if b, ok := d.value("live_ws_write_bytes_total"); ok {
			v := round(b/n, 0)
			sr.WriteBytesAvg = &v
		}
	}

	for _, sd := range segmentDefs {
		seg := Segment{Name: sd.key, Label: sd.label}
		p50, ok1 := d.histQuantile(sd.metric, 0.50)
		p99, ok2 := d.histQuantile(sd.metric, 0.99)
		if ok1 && ok2 {
			seg.Present = true
			seg.P50ms = round(p50*1000, 3)
			seg.P99ms = round(p99*1000, 3)
		}
		sr.Segments = append(sr.Segments, seg)
	}

	sr.KafkaOK = d.total("live_kafka_produce_total", "result", "ok")
	sr.KafkaErr = d.total("live_kafka_produce_total", "result", "error")

	// 空闲连接成本:建连前 → 发送开始前 这段只多了连接,还没有消息流量。
	// gauge 受 GC 时机影响,只能当近似值。
	if m.pre != nil && m.pre.serverErr == nil && aliveAtStart > 0 {
		per := func(name string, scale float64) *float64 {
			x0, ok0 := m.pre.server.value(name)
			x1, ok1 := m.before.server.value(name)
			if !ok0 || !ok1 {
				return nil
			}
			v := round((x1-x0)/float64(aliveAtStart)/scale, 2)
			return &v
		}
		sr.IdleRSSPerConnKB = per("process_resident_memory_bytes", 1024)
		sr.IdleHeapPerConnKB = per("go_memstats_heap_inuse_bytes", 1024)
		sr.GoroutinesPerConn = per("go_goroutines", 1)
	}
	return sr
}

func consumerResult(d promSnap, win float64) *ConsumerResult {
	cr := &ConsumerResult{
		OK:           d.total("live_consumer_messages_total", "result", "ok"),
		WriteErr:     d.total("live_consumer_messages_total", "result", "write_error"),
		UnmarshalErr: d.total("live_consumer_messages_total", "result", "unmarshal_error"),
	}
	cr.Rate = round(cr.OK/win, 1)
	if n := d.total("live_consumer_batch_size_count"); n > 0 {
		cr.BatchAvg = round(d.total("live_consumer_batch_size_sum")/n, 1)
	}
	if v, ok := d.histQuantile("live_consumer_lag_seconds", 0.99); ok {
		cr.LagP99s = round(v, 2)
	}
	if v, ok := d.histQuantile("live_consumer_write_duration_seconds", 0.99); ok {
		cr.WriteP99ms = round(v*1000, 1)
	}
	return cr
}

func (s *runState) timelineRows() []TimelineRow {
	if s.runStart.IsZero() {
		return nil
	}
	sendSec := -1
	if ns := s.sendStart.Load(); ns > 0 {
		sendSec = int(time.Duration(ns-s.runStart.UnixNano()) / time.Second)
	}
	durSecs := int(s.cfg.Duration / time.Second)
	last := min(int(time.Since(s.runStart)/time.Second), len(s.timeline)-1)

	rows := make([]TimelineRow, 0, last+1)
	for i := 0; i <= last; i++ {
		t := &s.timeline[i]
		phase := "ramp"
		if sendSec >= 0 && i >= sendSec {
			phase = "send"
			if i >= sendSec+durSecs {
				phase = "drain"
			}
		}
		rows = append(rows, TimelineRow{
			Sec: i, Phase: phase,
			Alive: t.alive.Load(), Sent: t.sent.Load(), Recv: t.recv.Load(),
			Skipped: t.skipped.Load(), SendErr: t.sendErr.Load(), Disconnects: t.disconnects.Load(),
			Lat: t.lat.summary(),
		})
	}
	return rows
}

// diagnose 把「数据本身可不可信」的判断自动化。
// 每一条告警都对应一个曾经踩过、或者很容易踩的坑。
func diagnose(r *RunResult, cfg Config) {
	warn := func(format string, args ...any) {
		r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
	}

	if r.Aborted {
		warn("本轮被中断,数据不完整")
	}

	for reason, n := range r.Load.FailReasons {
		switch {
		case strings.Contains(reason, "cannot assign requested address"):
			warn("建连失败 %d 条: 压测端临时端口耗尽。调大 -local-ips 或 net.ipv4.ip_local_port_range —— 这不是服务端的极限", n)
		case strings.Contains(reason, "too many open files"):
			warn("建连失败 %d 条: fd 用尽,先 ulimit -n 200000", n)
		case reason == "http 503":
			warn("建连失败 %d 条: 撞到服务端连接上限(max_ws_conns / max_conns_per_user),是人为设的天花板,不是机器极限", n)
		default:
			warn("建连失败 %d 条: %s", n, reason)
		}
	}

	if l := r.Load; l.Planned > 0 {
		if skipRatio := float64(l.Skipped) / float64(l.Planned); skipRatio > 0.01 {
			warn("压测端饱和: %.1f%% 的计划消息落后超过 -max-lag 被跳过。实际负载低于计划,减少 -conns 或换更强的压测机", skipRatio*100)
		} else if l.SendRatioPct < 99 && !r.Aborted {
			warn("只发出了计划的 %.1f%%(写失败 %d)", l.SendRatioPct, l.SendErr)
		}
		if l.SenderLag.P99 > 100 {
			warn("压测端发送滞后 p99 %.0f ms: 压测端调度跟不上,e2e 延迟里包含这一部分(看 service 延迟可以扣掉它)", l.SenderLag.P99)
		}
	}
	if r.Load.Disconnects > 0 {
		warn("运行中掉线 %d 条: 服务端写超时 / 读超时踢人,或进程崩溃,对照时间线定位是哪几秒", r.Load.Disconnects)
	}
	if r.Delivery.ErrorMsgs > 0 {
		warn("收到 %d 条 error 消息: 触发了服务端限流(每连接 2 条/s)或房间未开播", r.Delivery.ErrorMsgs)
	}

	if r.Client != nil && r.Server != nil && cfg.Local {
		used := r.Client.Cores + r.Server.Cores
		if used > 0.8*float64(r.Client.MachineCores) {
			warn("CPU 争用: 服务端 %.1f 核 + 压测端 %.1f 核 ≥ 机器 %d 核的 80%%。测到的是「谁抢到 CPU」,不是服务端能力",
				r.Server.Cores, r.Client.Cores, r.Client.MachineCores)
		}
	}

	sr := r.Server
	if sr == nil {
		return
	}
	if sr.Rejected > 0 {
		warn("服务端拒绝握手 %.0f 次: 撞到配置的连接上限", sr.Rejected)
	}
	if sr.DroppedQueueFullJobs > 0 {
		warn("广播池队列满 %.0f 次(每次 = 整个房间丢一条): 有 worker 被占满,典型的热点房间信号", sr.DroppedQueueFullJobs)
	}
	if p := sr.ClientVsServerPct; p != nil && *p < 95 {
		warn("客户端只收到服务端投递的 %.1f%%: 差额在内核缓冲里还没读到,或收尾时仍在途。调大 -drain 再看", *p)
	}
	present := 0
	for _, seg := range sr.Segments {
		if seg.Present {
			present++
		}
	}
	if present == 0 && r.Load.Sent > 0 {
		warn("服务端没有分段延迟指标: 服务端是旧版本,重新编译启动 server")
	}
	if b := sr.WriteBytesAvg; b != nil && *b > 1024 {
		warn("平均每次合并写 %.0f B > WriteBufferSize 1024 B: gorilla 会先把满的缓冲作为分片帧发出,「一次合并 = 一次 write」不再成立", *b)
	}
	if r.Consumer != nil && r.Consumer.WriteErr > 0 {
		warn("consumer 落库失败 %.0f 条", r.Consumer.WriteErr)
	}
}

// ---------- 输出 ----------

func (r *RunResult) print() {
	l, d := r.Load, r.Delivery
	fmt.Println()
	fmt.Printf("================ 结果: %s ================\n", r.Name)
	fmt.Printf("连接     尝试 %d | 成功 %d | 失败 %d | 建连 %.1fs | 发送开始时在线 %d | 结束时在线 %d | 运行中掉线 %d\n",
		l.Attempted, l.ConnectOK, l.ConnectFail, l.RampSecs, l.AliveAtStart, l.AliveAtEnd, l.Disconnects)
	if l.Planned > 0 {
		fmt.Printf("上行     发言者 %d | 计划 %.0f msg/s | 实际 %.0f msg/s(达成 %.1f%%) | 滞后跳过 %d | 写失败 %d | 发送滞后 p99 %.1fms\n",
			l.Senders, l.PlannedRate, l.ActualRate, l.SendRatioPct, l.Skipped, l.SendErr, l.SenderLag.P99)
		fmt.Printf("下行     期望 %d | 收到 %d | 丢失 %s | 客户端收 %s 次/s(扇出计数) | error 消息 %d\n",
			d.Expected, d.Received, fmtPct(d.LossPct), human(d.RecvRate), d.ErrorMsgs)
	}
	for _, g := range groupNames {
		gr := r.Groups[g]
		if gr == nil || gr.E2E.Count == 0 {
			continue
		}
		fmt.Printf("延迟[%-6s] %d 连接 | e2e p50 %s p90 %s p99 %s p99.9 %s max %s | service p50 %s p99 %s | 丢失 %s\n",
			g, gr.Conns, ms(gr.E2E.P50), ms(gr.E2E.P90), ms(gr.E2E.P99), ms(gr.E2E.P999), ms(gr.E2E.Max),
			ms(gr.Service.P50), ms(gr.Service.P99), fmtPct(gr.LossPct))
	}

	if sr := r.Server; sr != nil {
		fmt.Printf("服务端   窗口 %.0fs | CPU %.2f 核 | 下行 %s 次/s | 每核 %s 次/s | 合并写 %.2f 条/次",
			sr.WindowSecs, sr.Cores, human(sr.DownRate), human(sr.DeliveriesPerCoreSec), sr.WriteBatchAvg)
		if sr.WriteBytesAvg != nil {
			fmt.Printf(" %.0f B/次", *sr.WriteBytesAvg)
		}
		fmt.Println()
		fmt.Printf("         丢弃 client_slow %.0f(%.3f%%) | queue_full %.0f job | rate_limited %.0f | 拒绝握手 %.0f",
			sr.DroppedClientSlow, sr.DropPct, sr.DroppedQueueFullJobs, sr.DroppedRateLimited, sr.Rejected)
		if p := sr.ClientVsServerPct; p != nil {
			fmt.Printf(" | 客户端收到/服务端投递 %.1f%%", *p)
		}
		fmt.Println()
		fmt.Print("分段     ")
		for i, seg := range sr.Segments {
			if i > 0 {
				fmt.Print(" | ")
			}
			if seg.Present {
				fmt.Printf("%s p50 %s p99 %s", seg.Label, ms(seg.P50ms), ms(seg.P99ms))
			} else {
				fmt.Printf("%s -", seg.Label)
			}
		}
		fmt.Println()
		if sr.IdleRSSPerConnKB != nil {
			fmt.Printf("连接成本 每连接 RSS %.1f KB | heap %s KB | goroutine %s 个(建连前 → 发送前的差值)\n",
				*sr.IdleRSSPerConnKB, optF(sr.IdleHeapPerConnKB, 1), optF(sr.GoroutinesPerConn, 2))
		}
		if sr.KafkaOK+sr.KafkaErr > 0 {
			fmt.Printf("Kafka    produce ok %.0f | error %.0f\n", sr.KafkaOK, sr.KafkaErr)
		}
	}
	if c := r.Consumer; c != nil && c.OK+c.WriteErr > 0 {
		fmt.Printf("Consumer 落库 %.0f 条(%.0f/s) | 平均批 %.0f | 写库 p99 %.1fms | 消费延迟 p99 %.2fs | 写失败 %.0f\n",
			c.OK, c.Rate, c.BatchAvg, c.WriteP99ms, c.LagP99s, c.WriteErr)
	}
	if rd := r.Redis; rd != nil {
		fmt.Printf("Redis    命令 %.0f/s | 出流量 %.2f MB/s | 入流量 %.2f MB/s\n", rd.CommandsPerSec, rd.NetOutMBps, rd.NetInMBps)
	}
	if c := r.Client; c != nil {
		fmt.Printf("压测端   CPU %.2f 核(机器共 %d 核)\n", c.Cores, c.MachineCores)
	}
	for _, f := range r.Faults {
		status := "ok"
		if f.Error != "" {
			status = f.Error
		}
		fmt.Printf("故障注入 +%s %s → %s\n", f.At, f.Cmd, status)
	}
	for _, w := range r.Warnings {
		fmt.Println("⚠️  " + w)
	}
}

func writeRun(dir string, r *RunResult) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, r.Name+".json"), b, 0o644); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dir, r.Name+"-timeline.csv"))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"sec", "phase", "alive", "sent", "recv", "skipped", "send_err", "disconnects", "e2e_p50_ms", "e2e_p99_ms", "e2e_max_ms"})
	for _, t := range r.Timeline {
		_ = w.Write([]string{
			strconv.Itoa(t.Sec), t.Phase,
			i64(t.Alive), i64(t.Sent), i64(t.Recv), i64(t.Skipped), i64(t.SendErr), i64(t.Disconnects),
			f64(t.Lat.P50), f64(t.Lat.P99), f64(t.Lat.Max),
		})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ---------- 多轮汇总 ----------

type metricDef struct {
	label string
	get   func(*RunResult) (float64, bool)
}

func group(r *RunResult, g string) *GroupResult { return r.Groups[g] }

func segP99(r *RunResult, name string) (float64, bool) {
	if r.Server == nil {
		return 0, false
	}
	for _, s := range r.Server.Segments {
		if s.Name == name && s.Present {
			return s.P99ms, true
		}
	}
	return 0, false
}

func e2e(g string, pick func(Quantiles) float64) func(*RunResult) (float64, bool) {
	return func(r *RunResult) (float64, bool) {
		gr := group(r, g)
		if gr == nil || gr.E2E.Count == 0 {
			return 0, false
		}
		return pick(gr.E2E), true
	}
}

func server(pick func(*ServerResult) float64) func(*RunResult) (float64, bool) {
	return func(r *RunResult) (float64, bool) {
		if r.Server == nil {
			return 0, false
		}
		return pick(r.Server), true
	}
}

func optPtr(p *float64) (float64, bool) {
	if p == nil {
		return 0, false
	}
	return *p, true
}

var summaryMetrics = []metricDef{
	{"建连成功", func(r *RunResult) (float64, bool) { return float64(r.Load.ConnectOK), true }},
	{"发送开始时在线", func(r *RunResult) (float64, bool) { return float64(r.Load.AliveAtStart), true }},
	{"计划上行 msg/s", func(r *RunResult) (float64, bool) { return r.Load.PlannedRate, r.Load.Planned > 0 }},
	{"实际上行 msg/s", func(r *RunResult) (float64, bool) { return r.Load.ActualRate, r.Load.Planned > 0 }},
	{"发送达成率 %", func(r *RunResult) (float64, bool) { return r.Load.SendRatioPct, r.Load.Planned > 0 }},
	{"压测端发送滞后 p99 ms", func(r *RunResult) (float64, bool) { return r.Load.SenderLag.P99, r.Load.SenderLag.Count > 0 }},
	{"客户端收 次/s", func(r *RunResult) (float64, bool) { return r.Delivery.RecvRate, r.Load.Planned > 0 }},
	{"丢失率 %", func(r *RunResult) (float64, bool) { return optPtr(r.Delivery.LossPct) }},
	{"e2e p50 ms", e2e("normal", func(q Quantiles) float64 { return q.P50 })},
	{"e2e p99 ms", e2e("normal", func(q Quantiles) float64 { return q.P99 })},
	{"e2e p99.9 ms", e2e("normal", func(q Quantiles) float64 { return q.P999 })},
	{"e2e max ms", e2e("normal", func(q Quantiles) float64 { return q.Max })},
	{"service p99 ms", func(r *RunResult) (float64, bool) {
		gr := group(r, "normal")
		if gr == nil || gr.Service.Count == 0 {
			return 0, false
		}
		return gr.Service.P99, true
	}},
	{"慢组 e2e p99 ms", e2e("slow", func(q Quantiles) float64 { return q.P99 })},
	{"慢组 丢失率 %", func(r *RunResult) (float64, bool) {
		if gr := group(r, "slow"); gr != nil {
			return optPtr(gr.LossPct)
		}
		return 0, false
	}},
	{"迟到组 e2e p99 ms", e2e("late", func(q Quantiles) float64 { return q.P99 })},
	{"运行中掉线", func(r *RunResult) (float64, bool) { return float64(r.Load.Disconnects), true }},
	{"服务端 CPU 核", server(func(s *ServerResult) float64 { return s.Cores })},
	{"服务端下行 次/s", server(func(s *ServerResult) float64 { return s.DownRate })},
	{"每核投递 次/s", server(func(s *ServerResult) float64 { return s.DeliveriesPerCoreSec })},
	{"合并写 条/次", server(func(s *ServerResult) float64 { return s.WriteBatchAvg })},
	{"client_slow 丢弃", server(func(s *ServerResult) float64 { return s.DroppedClientSlow })},
	{"queue_full 丢弃(job)", server(func(s *ServerResult) float64 { return s.DroppedQueueFullJobs })},
	{"Redis PUBLISH p99 ms", func(r *RunResult) (float64, bool) { return segP99(r, "redis_publish") }},
	{"广播池排队 p99 ms", func(r *RunResult) (float64, bool) { return segP99(r, "queue_wait") }},
	{"deliver p99 ms", func(r *RunResult) (float64, bool) { return segP99(r, "deliver") }},
	{"合并写 p99 ms", func(r *RunResult) (float64, bool) { return segP99(r, "write") }},
	{"每连接 RSS KB", func(r *RunResult) (float64, bool) {
		if r.Server == nil {
			return 0, false
		}
		return optPtr(r.Server.IdleRSSPerConnKB)
	}},
	{"每连接 goroutine", func(r *RunResult) (float64, bool) {
		if r.Server == nil {
			return 0, false
		}
		return optPtr(r.Server.GoroutinesPerConn)
	}},
	{"Consumer 落库 msg/s", func(r *RunResult) (float64, bool) {
		if r.Consumer == nil || r.Consumer.OK == 0 {
			return 0, false
		}
		return r.Consumer.Rate, true
	}},
	{"Consumer 消费延迟 p99 s", func(r *RunResult) (float64, bool) {
		if r.Consumer == nil || r.Consumer.OK == 0 {
			return 0, false
		}
		return r.Consumer.LagP99s, true
	}},
	{"Redis 命令/s", func(r *RunResult) (float64, bool) {
		if r.Redis == nil {
			return 0, false
		}
		return r.Redis.CommandsPerSec, true
	}},
	{"压测端 CPU 核", func(r *RunResult) (float64, bool) {
		if r.Client == nil {
			return 0, false
		}
		return r.Client.Cores, true
	}},
}

// writeSummary 把多轮结果汇成一张表(每轮一列 + 中位数),写 summary.md 并打印。
// 取中位数而不是平均:单轮偶发的 GC / 调度抖动不该把结论拉偏。
func writeSummary(dir string, cfg Config, params map[string]string, preflight []string, rs []*RunResult) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# 压测汇总: %s\n\n", cfg.Scenario)
	fmt.Fprintf(&b, "- 时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&b, "- 目标: %s\n", cfg.Target)
	fmt.Fprintf(&b, "- 完成轮数: %d / %d\n", len(rs), cfg.Runs)
	fmt.Fprintf(&b, "- 压测机: %d 核\n", runtime.NumCPU())
	fmt.Fprintf(&b, "- 参数: `%s`\n\n", formatParams(params))

	if len(preflight) > 0 {
		b.WriteString("## 预检\n\n")
		for _, p := range preflight {
			fmt.Fprintf(&b, "- %s\n", p)
		}
		b.WriteString("\n")
	}

	b.WriteString("## 关键指标\n\n| 指标 |")
	for _, r := range rs {
		fmt.Fprintf(&b, " run%d |", r.Run)
	}
	b.WriteString(" 中位数 |\n|---|")
	for range rs {
		b.WriteString("---|")
	}
	b.WriteString("---|\n")

	for _, m := range summaryMetrics {
		vals := make([]float64, 0, len(rs))
		cells := make([]string, len(rs))
		for i, r := range rs {
			if v, ok := m.get(r); ok {
				vals = append(vals, v)
				cells[i] = fmtNum(v)
			} else {
				cells[i] = "-"
			}
		}
		if len(vals) == 0 {
			continue // 所有轮都没有的指标(比如没有慢客户端时的慢组)不出现在表里
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", m.label, strings.Join(cells, " | "), fmtNum(median(vals)))
	}

	var warned bool
	for _, r := range rs {
		for _, w := range r.Warnings {
			if !warned {
				b.WriteString("\n## 告警\n\n")
				warned = true
			}
			fmt.Fprintf(&b, "- run%d: %s\n", r.Run, w)
		}
	}

	b.WriteString("\n## 文件\n\n")
	for _, r := range rs {
		fmt.Fprintf(&b, "- `%s.json` 全部指标;`%s-timeline.csv` 逐秒时间线", r.Name, r.Name)
		for _, p := range r.Profiles {
			fmt.Fprintf(&b, ";`%s`", filepath.Base(p))
		}
		b.WriteString("\n")
	}

	out := b.String()
	fmt.Println()
	fmt.Println(out)
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(out), 0o644)
}

func formatParams(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("-%s=%s", k, params[k]))
	}
	return strings.Join(parts, " ")
}

// ---------- 小工具 ----------

func median(v []float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func lossPct(recv, expected int64) *float64 {
	if expected <= 0 {
		return nil
	}
	v := round((1-float64(recv)/float64(expected))*100, 3)
	return &v
}

func perSec(n, secs float64) float64 {
	if secs <= 0 {
		return 0
	}
	return n / secs
}

func round(v float64, digits int) float64 {
	p := math.Pow(10, float64(digits))
	return math.Round(v*p) / p
}

func fmtNum(v float64) string {
	switch a := math.Abs(v); {
	case a >= 100:
		return strconv.FormatFloat(v, 'f', 0, 64)
	case a >= 10:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return strconv.FormatFloat(v, 'f', 2, 64)
	}
}

// human 把大数写成「万」,方便和报告里「44.7 万次/s」的写法对照。
func human(v float64) string {
	if math.Abs(v) >= 1e4 {
		return strconv.FormatFloat(v/1e4, 'f', 1, 64) + "万"
	}
	return strconv.FormatFloat(v, 'f', 0, 64)
}

func ms(v float64) string { return fmtNum(v) + "ms" }

func fmtPct(p *float64) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatFloat(*p, 'f', 3, 64) + "%"
}

func optF(p *float64, digits int) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatFloat(*p, 'f', digits, 64)
}

func i64(v int64) string   { return strconv.FormatInt(v, 10) }
func f64(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
