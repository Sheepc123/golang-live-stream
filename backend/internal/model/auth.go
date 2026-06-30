package model

// User represents the internal User entity used by the backend.
type User struct {
	Username string
	Password string
	UserID   int64
}

type LoginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// LoginRequest represents the Json body sent by the clinet when logging in.
type LoginResponse struct {
	AccessToken string      `json:"access_token"`
	TokenType   string      `json:"token_type"`
	ExpiresIn   int64       `json:"expires_in"`
	User        UserProfile `json:"user"`
}

// UserProfile represents reprensents safe User information that can be returned by clients.
type UserProfile struct {
	Username string `json:"username"`
	UserID   int64  `json:"user_id"`
}
