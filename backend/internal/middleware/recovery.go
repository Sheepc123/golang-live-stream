package middleware

import (
	"net"
	"os"
	"strings"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Recovery 捕获 handler 里的 panic,替代 gin.Recovery()。
//
// 相比 gin.Recovery():
//  1. 日志走 zap,格式和其他日志统一(gin 自带的是标准库纯文本)
//  2. 带 trace_id 和完整堆栈
//  3. 返回项目统一的错误响应格式,而不是 gin 默认的空 500
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}

			// 客户端主动断开连接(用户关掉浏览器)时,
			// 往已关闭的 socket 写数据会 panic。
			// 这不是 bug,不该记成 Error,也没必要尝试返回响应 ——
			// 连接都没了,写什么都是徒劳。
			if isBrokenPipe(r) {
				logger.L().Debug("client connection broken",
					zap.String("path", c.Request.URL.Path),
					zap.Any("error", r),
				)
				c.Abort()
				return
			}

			// zap.Stack 抓取当前 goroutine 的完整调用栈。
			// 这个操作很贵,但 panic 本身就是异常情况,不考虑性能。
			logger.L().Error("panic recovered",
				zap.Any("error", r),
				zap.String("method", c.Request.Method),
				zap.String("path", c.Request.URL.Path),
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Stack("stack"),
			)

			response.Error(c, errno.InternalError)
			c.Abort()
		}()

		c.Next()
	}
}

// isBrokenPipe 判断 panic 是否由「对端已关闭连接」引起。
func isBrokenPipe(r any) bool {
	ne, ok := r.(*net.OpError)
	if !ok {
		return false
	}
	se, ok := ne.Err.(*os.SyscallError)
	if !ok {
		return false
	}
	msg := strings.ToLower(se.Error())
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer")
}
