package router

import (
	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/handler"
	"github.com/Sheepc123/golang-live-stream/internal/middleware"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/Sheepc123/golang-live-stream/internal/ws"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func NewRouter(cfg *config.Config, db *gorm.DB) *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger())

	// repo initalize
	userRepo := repo.NewUserRepo(db)
	roomRepo := repo.NewRoomRepo(db)
	msgRepo := repo.NewMesRep(db)

	// auth function
	authService := service.NewAuthService(cfg.JWT, userRepo)
	authHandler := handler.NewAuthHandler(authService)
	userHandler := handler.NewUserHandler()

	// roomService function
	roomService := service.NewRoomService(roomRepo)
	roomHandler := handler.NewRoomHandler(roomService)

	// Message function
	msgService := service.NewMsgService(msgRepo)
	msgHandler := handler.NewMsgHandler(msgService)

	// websocket funciton
	wsManager := ws.NewManager()
	wsHanlder := ws.NewWShandler(wsManager, cfg.JWT)

	api := r.Group("/api/v1")
	{
		auth := api.Group("/auth")
		{
			auth.POST("/login", authHandler.Login)
		}

		ws := api.Group("/ws")
		{
			ws.GET("/rooms/:room_id", wsHanlder.HandleRoomWebSocket)
		}

		protected := api.Group("")
		{
			//  all roundes in this group require a vaild JWT access toekn
			protected.Use(middleware.JWTAuth(cfg))

			// return the current authenticated user profile
			protected.GET("/user/me", userHandler.Me)

			rooms := protected.Group("/rooms")
			{
				rooms.GET("", roomHandler.ListRoom)

				rooms.GET("/:id", roomHandler.GetRoomByID)

				rooms.GET("/:id/messages", msgHandler.History)
			}

		}
	}

	r.NoRoute(func(c *gin.Context) {
		response.Fail(c, 404, errno.RouteNotFound.Code, errno.RouteNotFound.Msg, gin.H{})
	})
	return r
}
