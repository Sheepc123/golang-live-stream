package infra

import (
	"context"
	"errors"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// gormZapLogger 把 GORM 的日志转接到 zap。
//
// 为什么要写这个适配器:
// GORM 自带的 logger 直接往 stdout 打纯文本。
// 不接管的话,你的日志会一半 JSON、一半纯文本,
// 采集系统没法统一解析,trace_id 也串不起来。
type gormZapLogger struct {
	level         gormlogger.LogLevel
	slowThreshold time.Duration
}

// NewGormLogger 根据配置字符串创建 GORM logger。
// levelStr: silent / error / warn / info
func NewGormLogger(levelStr string) gormlogger.Interface {
	var lv gormlogger.LogLevel
	switch levelStr {
	case "silent":
		lv = gormlogger.Silent
	case "error":
		lv = gormlogger.Error
	case "info":
		lv = gormlogger.Info
	default:
		lv = gormlogger.Warn
	}

	return &gormZapLogger{
		level: lv,
		// 超过 200ms 的查询记为慢查询
		slowThreshold: 200 * time.Millisecond,
	}
}

func (l *gormZapLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	nl := *l
	nl.level = level
	return &nl
}

func (l *gormZapLogger) Info(ctx context.Context, msg string, args ...any) {
	if l.level >= gormlogger.Info {
		logger.FromCtx(ctx).Sugar().Infof(msg, args...)
	}
}

func (l *gormZapLogger) Warn(ctx context.Context, msg string, args ...any) {
	if l.level >= gormlogger.Warn {
		logger.FromCtx(ctx).Sugar().Warnf(msg, args...)
	}
}

func (l *gormZapLogger) Error(ctx context.Context, msg string, args ...any) {
	if l.level >= gormlogger.Error {
		logger.FromCtx(ctx).Sugar().Errorf(msg, args...)
	}
}

// Trace 是核心方法 —— 每执行一条 SQL 就调用一次。
//
// 性能要点:参数 fc 是个闭包,调用它才会拼出 SQL 字符串。
// 所以必须先判断级别,确认要记录了再调 fc() ——
// 否则在 4 万 INSERT/s 的消费端,光拼 SQL 字符串就能把 CPU 吃满。
// 这正是原来 logger.Default.LogMode(logger.Info) 的致命之处。
func (l *gormZapLogger) Trace(
	ctx context.Context,
	begin time.Time,
	fc func() (string, int64),
	err error,
) {
	if l.level <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)

	switch {
	// 情况一:SQL 出错。
	// ErrRecordNotFound 是正常业务流程(比如查用户不存在),不该记 Error。
	case err != nil && l.level >= gormlogger.Error &&
		!errors.Is(err, gorm.ErrRecordNotFound):
		sql, rows := fc()
		logger.FromCtx(ctx).Error("sql error",
			zap.Error(err),
			zap.Duration("elapsed", elapsed),
			zap.Int64("rows", rows),
			zap.String("sql", sql),
		)

	// 情况二:慢查询。生产环境最该盯的信号。
	case elapsed > l.slowThreshold && l.level >= gormlogger.Warn:
		sql, rows := fc()
		logger.FromCtx(ctx).Warn("slow sql",
			zap.Duration("elapsed", elapsed),
			zap.Duration("threshold", l.slowThreshold),
			zap.Int64("rows", rows),
			zap.String("sql", sql),
		)

	// 情况三:正常 SQL。只有显式开到 info 才记录,默认关闭。
	case l.level >= gormlogger.Info:
		sql, rows := fc()
		logger.FromCtx(ctx).Debug("sql",
			zap.Duration("elapsed", elapsed),
			zap.Int64("rows", rows),
			zap.String("sql", sql),
		)
	}
}
