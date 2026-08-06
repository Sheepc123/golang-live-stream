package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/gin-gonic/gin"
)

// TraceIDHeader 是链路 ID 的传递头。
// 用 X-Trace-Id 这个事实标准名,方便前端 / nginx / 其他服务对接。
const TraceIDHeader = "X-Trace-Id"

// Trace 为每个请求生成(或复用)链路 ID,写入 context 和响应头。
//
// 为什么需要:
// 一个请求可能在 handler / service / repo 三层各打一条日志,
// 高并发下这些日志在文件里是交错的,没有 traceID 就无法关联。
// 有了之后,拿用户报错时响应头里的 X-Trace-Id,
// 一条 grep 就能捞出这次请求的全部日志。
//
// 必须注册在最前面 —— 后面所有中间件和 handler 都依赖它。
func Trace() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 优先复用上游传下来的 ID(nginx / 前端 / 其他微服务生成的),
		// 这样跨服务调用时链路能串成一条。
		traceID := c.GetHeader(TraceIDHeader)
		if traceID == "" {
			traceID = newTraceID()
		}

		// 写回响应头,让客户端也能拿到 —— 用户报障时直接提供这个 ID
		c.Header(TraceIDHeader, traceID)

		// c.request is a pointer, point to ctx
		ctx := logger.WithTraceID(c.Request.Context(), traceID)
		c.Request = c.Request.WithContext(ctx)

		c.Set("trace_id", traceID)
		c.Next()
	}
}

// newTraceID 生成 16 字节随机 ID,输出 32 位十六进制。
//
// 格式刻意对齐 W3C Trace Context 规范里的 trace-id,
// 这样将来想接 OpenTelemetry 时可以平滑过渡。
// 不引 uuid 库 —— crypto/rand + hex 都是标准库,零依赖。
func newTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败极罕见(通常意味着系统熵源出问题)。
		// 降级用时间戳而不是 panic —— 日志 ID 不值得让服务挂掉。
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}
