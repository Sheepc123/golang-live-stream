package middleware

import (
	"strconv"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"github.com/gin-gonic/gin"
)

// Metrics 采集 HTTP 请求指标,和 Logger 中间件配合:
// Logger 记「这一次请求发生了什么」,Metrics 记「整体的量和耗时」。
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		// skipPaths 定义在同包的 logger.go 里,直接复用。
		// /metrics 被 Prometheus 每 15 秒抓一次,统计它自己没有意义。
		if skipPaths[c.Request.URL.Path] {
			c.Next()
			return
		}

		start := time.Now()

		// 先放行,等 handler 执行完再统计 —— 要拿最终状态码和耗时
		c.Next()

		// ⚠️ 这里是本文件唯一的难点:必须用 FullPath() 而不是 URL.Path
		//
		//	c.FullPath()         → "/api/v1/rooms/:id"    路由模板,取值有限
		//	c.Request.URL.Path   → "/api/v1/rooms/12345"  实际路径,取值无限
		//
		// Prometheus 里每个 label 取值组合都是一条独立时间序列。
		// 用实际路径的话,10 万个房间就是 10 万条序列,内存直接爆掉。
		// 这个坑叫「基数爆炸」(cardinality explosion)。
		path := c.FullPath()
		if path == "" {
			// FullPath 为空 = 没匹配到任何路由(404)。
			// 统一归成 unmatched,否则被扫描器一刷,
			// 每个乱七八糟的 URL 都会变成一条序列。
			path = "unmatched"
		}

		method := c.Request.Method
		status := strconv.Itoa(c.Writer.Status())

		metrics.HTTPRequests.WithLabelValues(method, path, status).Inc()
		metrics.HTTPDuration.WithLabelValues(method, path).
			Observe(time.Since(start).Seconds())
	}
}
