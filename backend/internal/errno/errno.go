package errno

type ErrorCode struct {
	Code int
	Msg  string
}

var (
	Success = ErrorCode{
		Code: 0,
		Msg:  "success",
	}

	InvalidRequest = ErrorCode{
		Code: 40001,
		Msg:  "invalid request",
	}

	UserAlreadyExists = ErrorCode{
		Code: 40002,
		Msg:  "User already exists",
	}

	UserNotFound = ErrorCode{
		Code: 40003,
		Msg:  "User not found",
	}

	WrongPassword = ErrorCode{
		Code: 40004,
		Msg:  "Wrong password",
	}

	InternalServerError = ErrorCode{
		Code: 40005,
		Msg:  "Internal Server Error",
	}

	RouteNotFound = ErrorCode{
		Code: 40006,
		Msg:  "Route not found",
	}

	Unauthorized = ErrorCode{
		Code: 40007,
		Msg:  "Unauthorized",
	}

	RoomNotFound = ErrorCode{
		Code: 40008,
		Msg:  "Room not found",
	}
)
