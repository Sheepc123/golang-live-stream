package middleware

import (
	"strings"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"github.com/gin-gonic/gin"
)

const jwtSecret = "dev-secret"

// RespFail Process the Unauthorized Error.
func respFail(c *gin.Context) {
	response.Fail(c, 401, errno.Unauthorized.Code, errno.Unauthorized.Msg, gin.H{})
	c.Abort()
}

// JWTAuth Verifies the access token before handlers run.
func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		// if authHeader must use the format : Bearer <token>
		if authHeader == "" {
			respFail(c)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)

		if len(parts) != 2 || parts[0] != "Bearer" {
			respFail(c)
			return
		}

		claims, err := Jwttoken.ParseAccessToken(parts[1], jwtSecret)

		if err != nil {
			respFail(c)
			c.Abort()
			return
		}
		c.Set("user_id", claims.UserID)
		c.Set("user_name", claims.Username)
		c.Next()
	}
}
