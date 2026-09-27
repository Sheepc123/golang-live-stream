package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// observer 在一轮的关键时刻给「压测端之外的世界」拍快照:
// 服务端 /metrics、consumer /metrics、Redis INFO、压测端自己的 CPU。
// 发送开始前一张、收尾后一张,两张做差就是这一轮窗口内的增量。
type observer struct {
	hc          *http.Client
	serverURL   string
	consumerURL string
	rdb         *redis.Client
}

type snapshot struct {
	at        time.Time     // 抓完服务端指标的时刻,作为窗口端点
	clientCPU time.Duration // 压测端进程累计 CPU 时间

	server      promSnap
	serverErr   error
	consumer    promSnap
	consumerErr error
	redis       map[string]float64
	redisErr    error
}

func (o *observer) take(ctx context.Context) *snapshot {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	s := &snapshot{}
	s.server, s.serverErr = scrapeProm(ctx, o.hc, o.serverURL)
	// 窗口端点取在「服务端 CPU 计数被读到」之后、紧挨着它 ——
	// 服务端核数 = ΔCPU / Δ时间,两个量要尽量在同一时刻取。
	s.at = time.Now()
	s.clientCPU = processCPU()

	if o.consumerURL != "" {
		s.consumer, s.consumerErr = scrapeProm(ctx, o.hc, o.consumerURL)
	}
	if o.rdb != nil {
		s.redis, s.redisErr = redisInfo(ctx, o.rdb)
	}
	return s
}

// redisInfo 读 INFO stats 里的计数器:total_commands_processed、
// total_net_output_bytes、pubsub_patterns 等。阶段 VI 对比动态订阅前后
// 的 Redis 流量时,看的就是这几个数。
func redisInfo(ctx context.Context, rdb *redis.Client) (map[string]float64, error) {
	txt, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, line := range strings.Split(txt, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			out[k] = f
		}
	}
	return out, nil
}

// processCPU 返回压测端进程累计的 用户态 + 内核态 CPU 时间。
//
// 为什么压测端也要报 CPU:同机压测时两边抢同一份 CPU。
// 服务端核数 + 压测端核数 接近机器总核数时,测出来的就不再是服务端的能力,
// 而是「谁先抢到 CPU」—— 报告会自动告警。
func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// noFileLimit 返回当前进程能打开的 fd 上限。
// Go 1.19 起程序启动时会自动把软上限提到硬上限,所以这里只读不改;
// 如果硬上限本身就低,只能在 shell 里 ulimit -n 调高。
func noFileLimit() (uint64, bool) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, false
	}
	return rl.Cur, true
}

// ephemeralPorts 返回每个源 IP 可用的临时端口数。
// docs/benchmark.md 5.1 节那 906 条建连失败就是它耗尽了。
func ephemeralPorts() (int, bool) {
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return 0, false
	}
	lo, err1 := strconv.Atoi(f[0])
	hi, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil || hi < lo {
		return 0, false
	}
	return hi - lo + 1, true
}

// fetchProfile 从服务端 pprof 端口下载一份 profile 存到 path。
// seconds > 0 时是 CPU profile,请求会阻塞 seconds 秒。
func fetchProfile(ctx context.Context, pprofURL, name string, seconds int, path string) error {
	u := strings.TrimRight(pprofURL, "/") + "/debug/pprof/" + name
	if seconds > 0 {
		u += "?seconds=" + strconv.Itoa(seconds)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second+30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: http %d", u, resp.StatusCode)
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// FaultEvent 记录一次故障注入命令的执行情况,写进结果文件。
type FaultEvent struct {
	At     string `json:"at"` // 相对发送开始的时刻
	Cmd    string `json:"cmd"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// runFaults 按时刻执行故障 / 恢复命令。
//
// 只要故障命令执行过,恢复命令就一定会执行 —— 哪怕中途 Ctrl+C。
// 否则一次中断的压测会把 Kafka 永远停在那里,后面每一轮的结果都是错的,
// 而且很难第一时间意识到。
func runFaults(ctx context.Context, cfg Config, sendStart time.Time) []FaultEvent {
	var events []FaultEvent
	run := func(cmd string) {
		at := time.Since(sendStart).Round(100 * time.Millisecond)
		fmt.Printf("  [故障注入 +%s] %s\n", at, cmd)
		out, err := runShell(cmd)
		ev := FaultEvent{At: at.String(), Cmd: cmd, Output: truncate(out, 500)}
		if err != nil {
			ev.Error = err.Error()
			fmt.Printf("  命令失败: %v %s\n", err, truncate(out, 200))
		}
		events = append(events, ev)
	}

	if !sleepCtx(ctx, time.Until(sendStart.Add(cfg.FaultAt))) {
		return events // 故障还没注入就被中断了,什么都不用恢复
	}
	run(cfg.FaultCmd)

	if cfg.RecoverCmd != "" {
		sleepCtx(ctx, time.Until(sendStart.Add(cfg.RecoverAt))) // 被中断也要往下走,执行恢复
		run(cfg.RecoverCmd)
	}
	return events
}

func runShell(cmd string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "sh", "-c", cmd).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// sleepCtx 睡 d,被 ctx 取消时提前返回 false。d ≤ 0 时立刻返回。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
