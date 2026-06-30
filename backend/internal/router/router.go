package router

import (
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/handler"
	"github.com/Sheepc123/golang-live-stream/internal/middleware"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/gin-gonic/gin"
)

func NewRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())

	authService := service.NewAuthService()
	authHandler := handler.NewAuthHandler(authService)
	userHandler := handler.NewUserHandler()

	roomService := service.NewRoomService()
	roomHandler := handler.NewRoomHandler(roomService)

	api := r.Group("/api/v1")
	{
		auth := api.Group("/auth")
		{
			auth.POST("/login", authHandler.Login)
		}

		protected := api.Group("")
		{
			//  all roundes in this group require a vaild JWT access toekn
			protected.Use(middleware.JWTAuth())

			// return the current authenticated user profile
			protected.GET("/user/me", userHandler.Me)

			rooms := protected.Group("/rooms")
			{
				rooms.GET("", roomHandler.ListRoom)

				rooms.GET("/:id", roomHandler.GetRoomByID)
			}

		}
	}

	r.NoRoute(func(c *gin.Context) {
		response.Fail(c, 404, errno.RouteNotFound.Code, errno.RouteNotFound.Msg, gin.H{})
	})
	return r
}
