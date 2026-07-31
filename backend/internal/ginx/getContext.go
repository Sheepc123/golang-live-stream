package ginx

import "github.com/gin-gonic/gin"

func GetContextValue[T any](c *gin.Context, key string) (T, bool) {
	value, ok := c.Get(key)

	if !ok {
		var zero T
		return zero, false
	}

	realValue, ok := value.(T)

	if !ok {
		var zero T
		return zero, false
	}

	return realValue, true
}
