// 压测客户端 v2。
//
// 在 backend/ 下:
//
//	go run ./cmd/benchmark -list                          # 列出预设场景
//	go run ./cmd/benchmark -scenario uniform -runs 3      # 均匀负载,跑 3 轮取中位数
//	go run ./cmd/benchmark -scenario hot                  # 热门房间
//	bash scripts/bench-suite.sh                           # 整套场景一键跑完
//
// JWT_SECRET 会自动从 backend/.env 读取,不用每次传 -jwt-secret。
// 使用说明和每个指标的含义见 docs/benchmark-guide.md。
//
// 相比 v1(已归档在 ./v1,用来复现 docs/benchmark.md 的历史数据):
//   - 开环发送:按计划时刻发,延迟从计划时刻算,纠正协调遗漏
//   - 预设场景:热门房间 / 进场风暴 / 慢客户端 / 空闲连接 / Kafka 故障注入
//   - 自动抓服务端 /metrics 前后快照做差:分段延迟、CPU 核数、每核投递、丢弃原因,
//     不用再每轮重启 server
//   - 顺带抓 consumer /metrics、Redis INFO、pprof CPU/heap
//   - 多轮运行 + 中位数汇总,结果落盘(JSON + 逐秒时间线 CSV + summary.md)
//   - 自动告警:压测端饱和、CPU 争用、端口耗尽、撞到连接上限……
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

// Config 是一轮压测的全部参数。flag 直接绑定到这些字段上,
// 场景预设通过 flag.Set 写入,所以预设和命令行走的是同一条路径。
type Config struct {
	Scenario string
	Target   string
	Local    bool // 目标是不是本机(决定源 IP 轮换、CPU 争用告警、各种 auto 地址)

	Conns, Rooms int
	RoomIDs      string
	Warm         int // >0 时先只连 Warm 条,发送开始后再涌入剩下的(进场风暴)
	Ramp         int
	StormRamp    int

	MsgRate  float64 // 每个发言者每秒几条
	Speakers float64 // 发言者比例
	Duration time.Duration
	Drain    time.Duration
	MaxLag   time.Duration

	SlowFrac  float64
	SlowDelay time.Duration

	Runs     int
	Cooldown time.Duration
	Seed     uint64
	Report   time.Duration

	JWTSecret string
	AdminUser string
	AdminPass string
	UserBase  int64
	LocalIPs  int

	MetricsURL    string
	ConsumerURL   string
	PprofURL      string
	ProfileSecs   int
	RedisAddr     string
	RedisPassword string

	FaultCmd   string
	FaultAt    time.Duration
	RecoverCmd string
	RecoverAt  time.Duration

	Out string
}

type scenario struct {
	name, desc string
	preset     map[string]string
}

// 预设只填「这个场景和别的场景不一样的地方」,其余走 flag 默认值。
// 命令行显式给出的参数永远优先于预设。
var scenarios = []scenario{
	{"uniform", "均匀负载:10 房间平分 3000 人、人人发言 0.5 条/s。和 docs/benchmark.md 的历史数据同口径",
		map[string]string{"conns": "3000", "rooms": "10", "msg-rate": "0.5", "speakers": "1"}},
	{"hot", "热门房间:1 个房间 5000 人,2% 的人发言 1 条/s(约 100 条/s 进房,50 万次/s 扇出)。打单房间串行扇出 + 全局锁",
		map[string]string{"conns": "5000", "rooms": "1", "msg-rate": "1", "speakers": "0.02"}},
	{"storm", "进场风暴:1 房间先进 1000 人稳定发言,稳定后剩下 4000 人以 2000 条/s 涌入。看进房写锁对广播的影响",
		map[string]string{"conns": "5000", "rooms": "1", "warm": "1000", "storm-ramp": "2000", "msg-rate": "1", "speakers": "0.05", "duration": "40s"}},
	{"slow", "慢客户端隔离:uniform 的负载,5% 的连接每读一帧睡 200ms。和 uniform 对比正常组有没有被拖累",
		map[string]string{"conns": "3000", "rooms": "10", "msg-rate": "0.5", "speakers": "1", "slow-frac": "0.05", "slow-delay": "200ms"}},
	{"idle", "空闲连接:5000 条只连不发,量每连接的内存和 goroutine 成本",
		map[string]string{"conns": "5000", "rooms": "10", "msg-rate": "0", "duration": "20s", "profile-secs": "0"}},
	{"kafka-down", "Kafka 故障注入:第 15s docker stop live-kafka,第 40s 再启动。看弹幕链路会不会被 Kafka 拖住",
		map[string]string{"conns": "1000", "rooms": "10", "msg-rate": "0.5", "speakers": "1",
			"fault-cmd": "docker stop live-kafka", "fault-at": "15s", "recover-cmd": "docker start live-kafka", "recover-at": "40s"}},
	{"custom", "不套预设,全部按命令行参数", nil},
}

func main() {
	// 在 backend/ 下运行时读到 .env,JWT_SECRET / REDIS_PASSWORD 就不用手传
	_ = godotenv.Load()

	var cfg Config
	flag.StringVar(&cfg.Scenario, "scenario", "uniform", "预设场景,-list 查看全部")
	flag.StringVar(&cfg.Target, "target", "http://127.0.0.1:8080", "服务端 HTTP 地址,WS 地址由它推导")

	flag.IntVar(&cfg.Conns, "conns", 1000, "总连接数")
	flag.IntVar(&cfg.Rooms, "rooms", 10, "房间数,连接按 i %% rooms 均分")
	flag.StringVar(&cfg.RoomIDs, "room-ids", "", "使用指定房间(逗号分隔,必须属于 admin);空 = 自动复用/创建 bench-room-*")
	flag.IntVar(&cfg.Warm, "warm", 0, ">0 时发送前只连这么多条,剩下的在发送阶段涌入(进场风暴)")
	flag.IntVar(&cfg.Ramp, "ramp", 200, "建连速率,条/s")
	flag.IntVar(&cfg.StormRamp, "storm-ramp", 1000, "进场风暴的建连速率,条/s")

	flag.Float64Var(&cfg.MsgRate, "msg-rate", 0.5, "每个发言者每秒发几条;0 = 只建连不发。服务端限流 2 条/s")
	flag.Float64Var(&cfg.Speakers, "speakers", 1, "发言者占在线连接的比例 [0,1]")
	flag.DurationVar(&cfg.Duration, "duration", 60*time.Second, "发送阶段时长")
	flag.DurationVar(&cfg.Drain, "drain", 5*time.Second, "停止发送后等多久再收尾,避免把在途消息算成丢失")
	flag.DurationVar(&cfg.MaxLag, "max-lag", time.Second, "发送落后计划超过这么久就跳过这一条(计入 skipped)")

	flag.Float64Var(&cfg.SlowFrac, "slow-frac", 0, "慢客户端比例 [0,1]")
	flag.DurationVar(&cfg.SlowDelay, "slow-delay", 200*time.Millisecond, "慢客户端每读一帧睡多久")

	flag.IntVar(&cfg.Runs, "runs", 1, "重复几轮,汇总取中位数")
	flag.DurationVar(&cfg.Cooldown, "cooldown", 10*time.Second, "两轮之间的间隔(让服务端连接、Kafka 积压回落)")
	flag.Uint64Var(&cfg.Seed, "seed", 1, "抽签种子:同一个种子每轮挑中同一批发言者和慢客户端")
	flag.DurationVar(&cfg.Report, "report", 5*time.Second, "运行中每隔多久打印一次进度")

	flag.StringVar(&cfg.JWTSecret, "jwt-secret", "", "和服务端相同的 JWT_SECRET;空 = 读环境变量 / backend/.env")
	flag.StringVar(&cfg.AdminUser, "admin-user", "bench_admin", "房主账号(用来建房开播),不存在会自动注册")
	flag.StringVar(&cfg.AdminPass, "admin-pass", "bench123456", "房主密码")
	flag.Int64Var(&cfg.UserBase, "user-base", 1_000_000, "压测用户 user_id 起始值,避开真实用户")
	flag.IntVar(&cfg.LocalIPs, "local-ips", -1, "轮换的本地源 IP 数(127.0.0.1..N);-1 = 本机目标用 4 个,远程目标用 1 个")

	flag.StringVar(&cfg.MetricsURL, "metrics", "auto", "服务端 /metrics 地址;auto = target + /metrics;off = 不抓")
	flag.StringVar(&cfg.ConsumerURL, "consumer-metrics", "auto", "consumer /metrics 地址;auto = 本机 :9101;off = 不抓")
	flag.StringVar(&cfg.PprofURL, "pprof", "auto", "服务端 pprof 地址;auto = 本机 :6060;off = 不抓")
	flag.IntVar(&cfg.ProfileSecs, "profile-secs", 20, "发送阶段中段抓多少秒 CPU profile;0 = 不抓")
	flag.StringVar(&cfg.RedisAddr, "redis", "auto", "Redis 地址,用来读 INFO stats;auto = 本机 :6379;off = 不读")
	flag.StringVar(&cfg.RedisPassword, "redis-password", "", "Redis 密码;空 = 读环境变量 REDIS_PASSWORD")

	flag.StringVar(&cfg.FaultCmd, "fault-cmd", "", "故障注入命令(sh -c 执行),比如 'docker stop live-kafka'")
	flag.DurationVar(&cfg.FaultAt, "fault-at", 15*time.Second, "发送开始后多久执行故障命令")
	flag.StringVar(&cfg.RecoverCmd, "recover-cmd", "", "恢复命令。只要故障命令执行过,即使 Ctrl+C 也一定会执行")
	flag.DurationVar(&cfg.RecoverAt, "recover-at", 40*time.Second, "发送开始后多久执行恢复命令")

	flag.StringVar(&cfg.Out, "out", "bench-results", "结果目录,每次运行在下面建一个 时间-场景 子目录")
	list := flag.Bool("list", false, "列出预设场景后退出")
	flag.Parse()

	if *list {
		printScenarios()
		return
	}
	if err := applyScenario(cfg.Scenario); err != nil {
		fatal(err)
	}
	if err := cfg.finish(); err != nil {
		fatal(err)
	}
	params := effectiveParams()

	// 第一次 Ctrl+C:当前轮提前结束,照样出报告、执行故障恢复命令;
	// 之后解除信号接管,第二次 Ctrl+C 直接杀掉进程。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	wsBase := wsURL(cfg.Target)
	hc := &http.Client{Timeout: 10 * time.Second}

	fmt.Printf("场景 %s: %s\n", cfg.Scenario, scenarioDesc(cfg.Scenario))
	obs := &observer{hc: hc, serverURL: cfg.MetricsURL, consumerURL: cfg.ConsumerURL}
	pre := preflight(ctx, cfg, obs)

	token, err := login(hc, cfg.Target, cfg.AdminUser, cfg.AdminPass)
	if err != nil {
		fatal(err)
	}
	rooms, err := prepareRooms(hc, cfg.Target, token, cfg.Rooms, cfg.RoomIDs)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("房间就绪: %v\n", rooms)

	dir := filepath.Join(cfg.Out, time.Now().Format("20060102-150405")+"-"+cfg.Scenario)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fatal(err)
	}

	var results []*RunResult
	for run := 1; run <= cfg.Runs; run++ {
		if run > 1 {
			fmt.Printf("\n冷却 %s …\n", cfg.Cooldown)
			if !sleepCtx(ctx, cfg.Cooldown) {
				break
			}
		}
		fmt.Printf("\n========== %s 第 %d/%d 轮 ==========\n", cfg.Scenario, run, cfg.Runs)
		r := runOnce(ctx, cfg, rooms, run, wsBase, obs, dir)
		r.Params = params
		r.print()
		if err := writeRun(dir, r); err != nil {
			fmt.Fprintln(os.Stderr, "写结果失败:", err)
		}
		results = append(results, r)
		if ctx.Err() != nil {
			break
		}
	}

	if len(results) > 0 {
		if err := writeSummary(dir, cfg, params, pre, results); err != nil {
			fmt.Fprintln(os.Stderr, "写汇总失败:", err)
		}
		fmt.Printf("结果目录: %s\n", dir)
	}
}

// applyScenario 把预设写进 flag —— 只写命令行没有显式给出的那些。
func applyScenario(name string) error {
	var sc *scenario
	for i := range scenarios {
		if scenarios[i].name == name {
			sc = &scenarios[i]
		}
	}
	if sc == nil {
		return fmt.Errorf("未知场景 %q,用 -list 查看", name)
	}
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	for k, v := range sc.preset {
		if explicit[k] {
			continue
		}
		if err := flag.Set(k, v); err != nil {
			return fmt.Errorf("场景 %s 的预设 %s=%s: %w", name, k, v, err)
		}
	}
	return nil
}

// finish 校验参数,并把 auto / off 解析成具体地址。
func (c *Config) finish() error {
	c.Target = strings.TrimRight(c.Target, "/")
	u, err := url.Parse(c.Target)
	if err != nil || u.Host == "" {
		return fmt.Errorf("-target 不合法: %q", c.Target)
	}
	c.Local = isLocalHost(u.Hostname())

	switch {
	case c.Conns <= 0, c.Rooms <= 0, c.Ramp <= 0, c.StormRamp <= 0, c.Runs <= 0:
		return fmt.Errorf("-conns / -rooms / -ramp / -storm-ramp / -runs 必须 > 0")
	case c.Warm < 0 || c.Warm >= c.Conns && c.Warm != 0:
		return fmt.Errorf("-warm 必须在 [0, conns) 之间")
	case c.MsgRate < 0:
		return fmt.Errorf("-msg-rate 不能为负")
	case c.Speakers < 0 || c.Speakers > 1, c.SlowFrac < 0 || c.SlowFrac > 1:
		return fmt.Errorf("-speakers / -slow-frac 必须在 [0,1]")
	case c.Duration <= 0:
		return fmt.Errorf("-duration 必须 > 0")
	}
	if c.FaultCmd != "" {
		if c.FaultAt >= c.Duration {
			return fmt.Errorf("-fault-at(%s)必须早于 -duration(%s)", c.FaultAt, c.Duration)
		}
		if c.RecoverCmd != "" && c.RecoverAt <= c.FaultAt {
			return fmt.Errorf("-recover-at 必须晚于 -fault-at")
		}
	}

	if c.JWTSecret == "" {
		c.JWTSecret = os.Getenv("JWT_SECRET")
	}
	if c.JWTSecret == "" {
		return fmt.Errorf("没有 JWT_SECRET:在 backend/ 下运行(自动读 .env),或传 -jwt-secret")
	}
	if c.RedisPassword == "" {
		c.RedisPassword = os.Getenv("REDIS_PASSWORD")
	}
	if c.LocalIPs < 0 {
		c.LocalIPs = 1
		if c.Local {
			c.LocalIPs = 4
		}
	}
	c.LocalIPs = max(c.LocalIPs, 1)

	c.MetricsURL = resolve(c.MetricsURL, c.Target+"/metrics", true)
	c.ConsumerURL = resolve(c.ConsumerURL, "http://127.0.0.1:9101/metrics", c.Local)
	c.PprofURL = resolve(c.PprofURL, "http://127.0.0.1:6060", c.Local)
	c.RedisAddr = resolve(c.RedisAddr, "127.0.0.1:6379", c.Local)
	return nil
}

// resolve:"off" → 空;"auto" → local 为真时用 def,否则空;其他值原样返回。
func resolve(v, def string, local bool) string {
	switch v {
	case "off", "":
		return ""
	case "auto":
		if local {
			return def
		}
		return ""
	}
	return v
}

// preflight 在开跑前检查环境,把「一定会出问题」的配置提前说出来。
// 返回的提示会写进 summary.md。
func preflight(ctx context.Context, cfg Config, obs *observer) []string {
	var notes []string
	note := func(format string, args ...any) {
		s := fmt.Sprintf(format, args...)
		notes = append(notes, s)
		fmt.Println("预检: " + s)
	}

	note("压测机 %d 核,目标 %s(本机: %v),源 IP %d 个", runtime.NumCPU(), cfg.Target, cfg.Local, cfg.LocalIPs)

	if lim, ok := noFileLimit(); ok && lim < uint64(cfg.Conns)+256 {
		note("⚠️ fd 上限 %d < 连接数 %d:先 ulimit -n 200000", lim, cfg.Conns)
	}
	if ports, ok := ephemeralPorts(); ok && cfg.Local && ports*cfg.LocalIPs < cfg.Conns+1000 {
		note("⚠️ 临时端口 %d × %d 个源 IP 不够 %d 条连接:调大 -local-ips 或 ip_local_port_range", ports, cfg.LocalIPs, cfg.Conns)
	}
	if cfg.MsgRate > 2 {
		note("⚠️ -msg-rate %.1f 超过服务端限流 2 条/s,超出部分会收到 error 消息", cfg.MsgRate)
	}
	if cfg.FaultCmd != "" {
		note("本场景会执行故障命令 %q(+%s)和恢复命令 %q(+%s)。进程被强杀时请手动执行恢复命令",
			cfg.FaultCmd, cfg.FaultAt, cfg.RecoverCmd, cfg.RecoverAt)
	}

	if cfg.MetricsURL != "" {
		snap, err := scrapeProm(ctx, obs.hc, cfg.MetricsURL)
		switch {
		case err != nil:
			note("⚠️ 服务端指标不可用(%v):CPU、分段延迟、每核投递、丢弃原因都会缺失", err)
			obs.serverURL = ""
		default:
			if _, ok := snap.value("live_broadcast_queue_wait_seconds_count"); !ok {
				note("⚠️ 服务端没有分段延迟指标:server 是旧版本,重新编译启动后再测")
			}
		}
	}
	if cfg.ConsumerURL != "" {
		if _, err := scrapeProm(ctx, obs.hc, cfg.ConsumerURL); err != nil {
			note("consumer 指标不可用(没启动 consumer?),不统计落库")
			obs.consumerURL = ""
		}
	}
	if cfg.RedisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := rdb.Ping(pctx).Err()
		cancel()
		if err != nil {
			note("Redis %s 连不上(%v),不统计 Redis 流量", cfg.RedisAddr, err)
			_ = rdb.Close()
		} else {
			obs.rdb = rdb
		}
	}
	if cfg.PprofURL == "" {
		note("不抓 pprof")
	}
	return notes
}

// effectiveParams 记录每个参数的最终值(含场景预设),写进结果文件,保证可复现。
// 机密参数打码。
func effectiveParams() map[string]string {
	out := map[string]string{}
	flag.VisitAll(func(f *flag.Flag) {
		switch f.Name {
		case "list":
			return
		case "jwt-secret", "redis-password", "admin-pass":
			if f.Value.String() != "" {
				out[f.Name] = "***"
			}
			return
		}
		out[f.Name] = f.Value.String()
	})
	return out
}

func printScenarios() {
	fmt.Println("预设场景(命令行显式给出的参数会覆盖预设):")
	for _, s := range scenarios {
		fmt.Printf("\n  %-11s %s\n", s.name, s.desc)
		if len(s.preset) > 0 {
			fmt.Printf("  %-11s %s\n", "", formatParams(s.preset))
		}
	}
}

func scenarioDesc(name string) string {
	for _, s := range scenarios {
		if s.name == name {
			return s.desc
		}
	}
	return ""
}

func wsURL(target string) string {
	u, _ := url.Parse(target)
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	return u.String()
}

func isLocalHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
