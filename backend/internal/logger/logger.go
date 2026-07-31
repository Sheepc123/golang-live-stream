package logger

import (
	"fmt"
	"os"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	l      = zap.NewNop()
	s      = l.Sugar()
	syncer zapcore.WriteSyncer
)

func Init(cfg config.LogConfig) error {
	// ParseLevel support "debug"/"info"/"warn"/"error"

	level, err := zapcore.ParseLevel(cfg.Level)

	if err != nil {
		return fmt.Errorf("parse log level %q: %w", cfg.Level, err)
	}

	encCfg := zapcore.EncoderConfig{
		TimeKey:       "ts",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stack",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder,

		// ISO8601 是人和机器都能读的格式,比 Unix 时间戳友好
		EncodeTime: zapcore.ISO8601TimeEncoder,

		// duration 输出成毫秒数字(如 12.5),便于日志系统做聚合统计
		EncodeDuration: zapcore.MillisDurationEncoder,
		// ShortCaller 只输出 "ws/manager.go:83",不带完整路径
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	var (
		encoder zapcore.Encoder
		ws      zapcore.WriteSyncer
	)

	stdout := zapcore.Lock(os.Stdout)

	switch cfg.Format {
	case "console":
		// 开发环境:彩色大写级别 + 人类可读,且不做缓冲 ——
		// 调试时要求日志立刻出现在终端上。
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encCfg)
		ws = stdout

	default: // "json"
		encoder = zapcore.NewJSONEncoder(encCfg)

		// 生产/压测环境:加一层缓冲写入器。
		//
		// 为什么需要:每次 Write 都是一次 write 系统调用(微秒级)。
		// 高负载下即使只有 Info 级别日志,QPS 上万时 syscall 也会成为瓶颈。
		// 缓冲到 256KB 或满 5 秒才真正写一次,syscall 次数降几个数量级。
		//
		ws = &zapcore.BufferedWriteSyncer{
			WS:            stdout,
			Size:          256 * 1024,
			FlushInterval: 5 * time.Second,
		}
	}

	syncer = ws

	l = zap.New(
		zapcore.NewCore(encoder, ws, level),
		zap.AddCaller(),
		// Error 及以上自动附带堆栈。
		// 不要设成 Warn —— 抓堆栈很贵,而本项目 Warn 的量不小。
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	s = l.Sugar()

	return nil
}

// L 返回原始 *zap.Logger,给热路径使用
//
// 热路径 = 每条弹幕/每次广播都会执行到的代码,例如:
//   - ws.Manager.deliver
//   - ws.BroadcastPool.runWoker
//   - ws.Client.ReadPump / WritePump
//   - consumer 的消费循环
func L() *zap.Logger { return l }

// S 返回 SugaredLogger,给需要 printf 风格的冷路径使用。
func S() *zap.SugaredLogger { return s }

// 如果参数只是 zap.Int64(id) 这种零成本构造,直接调 Debug 即可,不用套判断。
func DebugEnabled() bool {
	return l.Core().Enabled(zapcore.DebugLevel)
}

// Sync 把缓冲区里的日志强制刷出。必须在进程退出前调用。
//
// 忽略返回值是刻意的:当输出是终端或管道时,
// 底层 fsync 会返回 "sync /dev/stdout: invalid argument",
// 这是 Linux 上的已知行为,不是真的错误。
func Sync() {
	if syncer != nil {
		_ = syncer.Sync()
	}
	_ = l.Sync()
}
