package middleware

import (
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// skipPaths 是不记录访问日志、不采集指标的路径。
//
// /metrics 会被 Prometheus 每 15 秒抓一次,健康检查更频繁。
// 这些日志没有任何价值,只会淹没真正有用的记录。
var skipPaths = map[string]bool{
	"/metrics": true,
	"/healthz": true,
	"/readyz":  true,
}

// Logger 记录结构化访问日志,替代 gin.Logger()。
//
// 相比 gin.Logger() 的改进:
//  1. 结构化输出(JSON),日志系统能按字段查询聚合,不用正则解析
//  2. 带 trace_id,能和业务日志关联
//  3. 按响应状态分级:5xx→Error、4xx→Warn、其余→Info
//     这样告警只需盯 Error 级别,不会被正常流量淹没
//  4. 跳过 /metrics 等噪音路径
func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		if skipPaths[c.Request.URL.Path] {
			c.Next()
			return
		}

		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// 先放行,等 handler 执行完再记录 —— 因为要拿最终 status 和耗时
		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.Int("status", status),
			// zap 的 duration 编码器配成了毫秒数字,
			// 输出是 "latency":12.5 而不是 "12.5ms" —— 便于做 P99 统计
			zap.Duration("latency", latency),
			zap.String("ip", c.ClientIP()),
			zap.Int("size", c.Writer.Size()),
			zap.String("trace_id", c.GetString("trace_id")),
		}

		// gin 允许 handler 用 c.Error(err) 挂载错误,这里一并输出
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		switch {
		case status >= 500:
			// 服务端错误:一定是我们的 bug,必须告警
			logger.L().Error("http request", fields...)
		case status >= 400:
			// 客户端错误:参数错、未登录、无权限 —— 属正常业务流程,
			// 但量突然变大时值得关注(比如接口改了没通知前端)
			logger.L().Warn("http request", fields...)
		default:
			logger.L().Info("http request", fields...)
		}
	}
}
