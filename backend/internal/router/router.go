package router

import (
	"net/http"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/handler"
	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/middleware"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	"github.com/Sheepc123/golang-live-stream/internal/service"
	"github.com/Sheepc123/golang-live-stream/internal/ws"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func NewRouter(
	cfg *config.Config,
	db *gorm.DB,
	rdb *redis.Client,
	producer *infra.KafkaProducer,
) (*gin.Engine, *ws.Manager, *ws.Aggregator) {

	r := gin.New()
	r.Use(
		middleware.Trace(),
		middleware.Logger(),
		middleware.Recovery(),
		middleware.Metrics(),
	)

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// 健康检查:K8s 的 liveness / readiness 探针会用
	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// repo initalize
	userRepo := repo.NewUserRepo(db)
	roomRepo := repo.NewRoomRepo(db)
	msgRepo := repo.NewMesRep(db)
	lsRepo := repo.NewLiveSessionRepo(db)

	// auth function
	authService := service.NewAuthService(cfg.JWT, userRepo)
	authHandler := handler.NewAuthHandler(authService)
	userHandler := handler.NewUserHandler()

	// RoomService need the LikeCounter interface
	LikeCounter := live.NewLikeCounter(rdb)
	onlineCont := live.NewOnlineCounter(rdb)

	// roomService function
	roomService := service.NewRoomService(roomRepo, onlineCont)
	roomHandler := handler.NewRoomHandler(roomService)

	// Message function
	msgService := service.NewMsgService(msgRepo)

	// ---------- 实验开关 ----------
	// 两个 legacy_* 开关的零值都是「生产行为」,配置漏写不会退化。
	// 开着的时候打 Warn,防止有人在压测机上开了对照组忘记关。

	// 实验 B:弹幕落库走 Kafka(生产)还是同步写 MySQL(对照组)。
	var sink ws.MsgSink

	if cfg.Experiment.LegacySyncDBWrite {
		sink = ws.NewSyncDBSink(msgRepo)
		logger.L().Warn("experiment enabled: legacy_sync_db_write (chat bypasses Kafka)")
	} else {
		sink = ws.NewKafkaSink(producer)
	}

	// 实验 C:点赞/进出场即时广播(对照组)还是每秒聚合(生产)。
	instant := cfg.Experiment.LegacyInstantCounters
	if instant {
		logger.L().Warn("experiment enabled: legacy_instant_counters (aggregator disabled)")
	}

	// websocket funciton
	wsReistry := ws.NewActionRegistry()
	wsManager := ws.NewManager(rdb, sink, cfg.Server)

	// live function
	SMgr := live.NewSessionManager(lsRepo, LikeCounter, rdb, onlineCont)
	ls := live.NewLiveService(SMgr, roomRepo, wsManager)
	liveHandler := live.NewLiveHandler(ls)

	msgHandler := handler.NewMsgHandler(msgService, SMgr)

	// register actions
	// register ChatAction
	wsReistry.Register(ws.MessageTypeChat, ws.NewChatAction(wsManager, SMgr))

	// register LikeAction
	wsReistry.Register(ws.MessageTypeLike, ws.NewLikeAction(wsManager, LikeCounter, SMgr, instant))
	wsHanlder := ws.NewWShandler(wsManager, cfg.JWT, wsReistry, SMgr, instant)

	// Aggregator  periodically publish like and online counts.
	// It scans locally roomID and updates only when values change
	//
	// It reads RoomID only from the local pool，
	// without publishing them through redis.
	//
	// In a multi-instance deployment, each instance reads the same global
	// counters from Redis and pushes updates only to its own local connections.
	aggregator := ws.NewAggregator(wsManager, SMgr)
	if !instant {
		aggregator.Start()
	}

	api := r.Group("/api/v1")
	{
		auth := api.Group("/auth")
		{
			auth.POST("/login", authHandler.Login)
			auth.POST("/register", authHandler.Register)
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

				rooms.GET("/mine", roomHandler.ListMyRoom)

				rooms.GET("/:id", roomHandler.GetRoomByID)
				rooms.PUT("/:id", roomHandler.UpdateRoom)
				rooms.POST("", roomHandler.CreatRoom)
				rooms.DELETE("/:id", roomHandler.DeleteRoom)

				rooms.GET("/:id/messages", msgHandler.History)

				rooms.POST("/:id/live/start", liveHandler.LiveStart)
				rooms.POST("/:id/live/stop", liveHandler.LiveStop)
			}

		}
	}

	r.NoRoute(func(c *gin.Context) {
		response.Error(c, errno.RouteNotFound)
	})
	return r, wsManager, aggregator
}
