package middleware

import (
	"strings"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"github.com/gin-gonic/gin"
)

// JWTAuth Verifies the access token before handlers run.
func JWTAuth(Cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		// if authHeader must use the format : Bearer <token>
		if authHeader == "" {
			response.Abort(c, errno.Unauthorized)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)

		if len(parts) != 2 || parts[0] != "Bearer" {
			response.Abort(c, errno.Unauthorized)
			return
		}

		jwtSecret := Cfg.JWT.Secret

		claims, err := Jwttoken.ParseAccessToken(parts[1], jwtSecret)

		if err != nil {
			response.Abort(c, errno.InvalidToken)
			return
		}
		c.Set("user_id", claims.UserID)
		c.Set("user_name", claims.Username)
		c.Next()
	}
}
