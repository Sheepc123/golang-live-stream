package Jwttoken

import (
	"errors"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"
)

var ErrInvalidToken = errors.New("invalid token")

type Claims struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
	jwtlib.RegisteredClaims
}

// GenerateAcessToken creats a signed JWT access token.
func GenerateAccessToken(userID int64, username string, secret string, expiresIn time.Duration) (string, error) {
	now := time.Now()

	claims := Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwtlib.RegisteredClaims{
			IssuedAt:  jwtlib.NewNumericDate(now),
			ExpiresAt: jwtlib.NewNumericDate(now.Add(expiresIn)),
		},
	}

	token := jwtlib.NewWithClaims(jwtlib.SigningMethodHS256, claims)

	return token.SignedString([]byte(secret))
}

func ParseAccessToken(tokenString string, secret string) (*Claims, error) {
	claims := &Claims{}

	parsedToken, err := jwtlib.ParseWithClaims(tokenString, claims, func(token *jwtlib.Token) (any, error) {
		// Only allow HMAC signing methods for this project.
		if _, ok := token.Method.(*jwtlib.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}

		return []byte(secret), nil
	})

	if err != nil {
		return nil, err
	}

	if !parsedToken.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}
