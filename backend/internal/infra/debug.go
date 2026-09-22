package infra

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"go.uber.org/zap"
)

// StartPprof 在独立端口上启动 pprof。port 为空则什么都不做。
//
// ── 为什么必须独立端口 ──
// pprof 暴露的东西:
//
//	/debug/pprof/goroutine  全部 goroutine 的调用栈(泄漏内部路径)
//	/debug/pprof/heap       堆快照(可能含内存里的敏感数据)
//	/debug/pprof/profile    默认采样 30 秒 CPU —— 被人反复调用
//	                        就是一个免费的 DoS 入口
//
// 挂在 8080 上等于把这些放到公网。放独立端口后,
// 部署时用防火墙/K8s NetworkPolicy 只放行内网即可。
//
// ── 压测时最该看的三个 ──
//  1. /debug/pprof/profile → CPU 火焰图。这是决定 P2 做哪一项的
//     唯一依据:大头在 syscall.Write 就做 III.8(合并写);
//     在 runtime.mapaccess / 锁上就做 map 分片;
//     在 json.Marshal 上就换序列化库。
//  2. /debug/pprof/heap → 验证「单连接 < 1 KB」这条验收标准。
//  3. /debug/pprof/goroutine?debug=1 → goroutine 总数应该
//     ≈ 2×连接数 + 常量(每连接一个 ReadPump + 一个 WritePump)。
//     偏离就是泄漏。
func StartPprof(port string) {
	if port == "" {
		logger.L().Info("pprof disabled")
		return
	}

	mux := http.NewServeMux()
	// 不用 http.DefaultServeMux —— net/http/pprof 的 init() 会
	// 自动往 DefaultServeMux 注册。如果业务代码哪天不小心用了
	// DefaultServeMux 起了个对外服务,pprof 就跟着漏出去了。
	// 显式注册到私有 mux,把这个隐式行为掐掉。
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
		// profile 默认采样 30 秒,trace 也可能很久。
		// WriteTimeout 必须放宽,否则 profile 拉到一半被掐断。
		WriteTimeout: 2 * time.Minute,
		ReadTimeout:  30 * time.Second,
	}

	go func() {
		logger.L().Info("pprof listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// pprof 起不来不该拖垮业务,记 Error 就够
			logger.L().Error("pprof server error", zap.Error(err))
		}
	}()
}
