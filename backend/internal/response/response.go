package response

import (
	"errors"
	"log"

	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/gin-gonic/gin"
)

type Response struct {
	Code int    `json:"code"`
	Data any    `json:"data"`
	Msg  string `json:"msg"`
}

// code = 0  success
func Ok(c *gin.Context, data any) {
	c.JSON(errno.Success.Status, Response{
		Code: errno.Success.Code,
		Msg:  errno.Success.Msg,
		Data: data,
	})
}

func Error(c *gin.Context, err error) {
	var ec errno.ErrorCode

	if !errors.As(err, &ec) {
		log.Printf("unclassified error on %s %s: %v", c.Request.Method, c.Request.URL.Path, err)
		ec = errno.InternalError
	}

	c.JSON(ec.Status, Response{Code: ec.Code, Msg: ec.Msg, Data: gin.H{}})
}

func ErrorWithData(c *gin.Context, err error, data any) {
	var ec errno.ErrorCode
	if !errors.As(err, &ec) {
		log.Printf("unclassified error on %s %s: %v", c.Request.Method, c.Request.URL.Path, err)
		ec = errno.InternalError
	}
	c.JSON(ec.Status, Response{Code: ec.Code, Msg: ec.Msg, Data: data})
}

// Abort writes the error and stops the middleware chain.
func Abort(c *gin.Context, err error) {
	Error(c, err)
	c.Abort()
}
