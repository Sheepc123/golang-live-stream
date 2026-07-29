package errno

import "net/http"

// ErrCode is both an API error code and a GO error.

// Because it implements error, the repo/service layer can return it

// and the handler layer can render it without any type switch

type ErrorCode struct {
	Code   int
	Msg    string
	Status int //HTTP stauts code
}

// error is a interface
// implements Error method
func (e ErrorCode) Error() string { return e.Msg }

func newCode(code int, msg string, status int) ErrorCode {
	return ErrorCode{Code: code, Msg: msg, Status: status}
}

// ======0: success========

var Success = newCode(0, "success", http.StatusOK)

// =======1xxxx:general/system=======
var (
	InvalidRequest = newCode(10001, "invalid request", http.StatusBadRequest)
	RouteNotFound  = newCode(10002, "route not found", http.StatusNotFound)
	InternalError  = newCode(10003, "internal server error", http.StatusInternalServerError)
)

// =======2XXXX:auth==========
var (
	Unauthorized       = newCode(20001, "unauthorized", http.StatusUnauthorized)
	InvalidToken       = newCode(20002, "invalid or expired token", http.StatusUnauthorized)
	InvalidCredentials = newCode(20003, "incorrect username or password", http.StatusUnauthorized)
)

// =====3XXXX:user====
var (
	UserNotFound      = newCode(30001, "user not found", http.StatusNotFound)
	UserAlreadyExists = newCode(30002, "user already exists", http.StatusConflict)
)

// ===== 4xxxx: room =====
var (
	RoomNotFound  = newCode(40001, "room not found", http.StatusNotFound)
	RoomForbidden = newCode(40002, "permission denied: not the room owner", http.StatusForbidden)
)

// ===== 5xxxx: live session =====
var (
	LiveAlreadyStarted = newCode(50001, "live session already active", http.StatusConflict)
	LiveNotActive      = newCode(50002, "live session not active", http.StatusConflict)
)

// ===== 6xxxx: message / websocket =====
var (
	MessageTooLong = newCode(60001, "message too long", http.StatusBadRequest)
)
