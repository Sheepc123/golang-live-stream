package logger

import (
	"context"

	"go.uber.org/zap"
)

type ctxKey struct{}

var traceIDKey ctxKey

// Generate TraceId into context traceIDkey ctxkey is a null struct 
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFrom 从 context 取出 traceID,取不到返回空串。
func TraceIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(traceIDKey).(string)
	return id
}

// return a new logger with a trace_id
func FromCtx(ctx context.Context) *zap.Logger {
	if id := TraceIDFrom(ctx); id != "" {
		return l.With(zap.String("trace_id", id))
	}
	return l
}
