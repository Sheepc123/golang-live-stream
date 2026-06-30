package response

import "github.com/gin-gonic/gin"

type Response struct {
	Code int    `json:"code"`
	Data any    `json:"data"`
	Msg  string `json:"msg"`
}

// code = 0  success
func Ok(c *gin.Context, data any) {
	c.JSON(200, Response{
		Code: 0,
		Msg:  "success",
		Data: data,
	})
}

func OKWithMsg(c *gin.Context, msg string, data any) {
	c.JSON(200, Response{
		Code: 0,
		Msg:  msg,
		Data: data,
	})
}

func Fail(c *gin.Context, httpStatus int, code int, msg string, data any) {
	c.JSON(httpStatus, Response{
		Code: code,
		Msg:  msg,
		Data: data,
	})
}

func FailWithData(c *gin.Context, httpStatus int, code int, msg string, data any) {
	c.JSON(httpStatus, Response{
		Code: code,
		Msg:  msg,
		Data: data,
	})
}
