package ws

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type WSHandler struct {
	manager    *Manager
	jwtSecret  string
	registry   *ActionRegistry
	SessionMgr *live.SessionManager
}

func NewWShandler(
	m *Manager,
	jwtcfg config.JWTConfig,
	registry *ActionRegistry,
	SessionMgr *live.SessionManager,
) *WSHandler {
	return &WSHandler{
		manager:    m,
		jwtSecret:  jwtcfg.Secret,
		registry:   registry,
		SessionMgr: SessionMgr,
	}
}

// HandleRoomWebSocket handle the connection through websocket
// GET /ws/rooms/:id
func (h *WSHandler) HandleRoomWebSocket(c *gin.Context) {

	accessToken := c.Query("token")

	if accessToken == "" {
		response.Error(c, errno.Unauthorized)
		return
	}

	// get userId and Username through token
	claims, err := Jwttoken.ParseAccessToken(accessToken, h.jwtSecret)
	if err != nil {
		response.Error(c, errno.Unauthorized)
		return
	}
	//Get roomId through url
	roomId, err := strconv.ParseInt(c.Param("room_id"), 10, 64)
	if err != nil {
		response.Error(c, errno.InvalidRequest)
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.L().Warn("websocket upgrade fail",
			zap.Int64("room_id", roomId),
			zap.Int64("user_id", claims.UserID),
			zap.String("ip", c.ClientIP()),
			zap.Error(err),
		)
		return
	}

	userId := claims.UserID
	username := claims.Username
	client := NewClient(roomId, userId, username, conn)

	h.manager.TrackConn()
	defer h.manager.UnTrackConn()

	defer func() {
		h.manager.Unregister(client)
		client.Close()

		h.manager.NoteLeave(roomId,username)
		
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		
		h.SessionMgr.ViewerLeave(ctx, roomId, userId)
		cancel()

		logger.L().Debug(
			"user left room",
			zap.Int64("room_id", roomId),
			zap.Int64("user_id", userId),
			zap.String("username", username),
		)

	}()
	// Register Manager
	h.manager.Register(client)

	logger.L().Debug(
		"user joined room",
		zap.Int64("room_id", roomId),
		zap.Int64("user_id", userId),
		zap.String("username", username),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	onlineCount := h.SessionMgr.ViewerJoin(ctx, roomId, userId)

	currentLikeCount, _ := h.SessionMgr.GetLikeCount(ctx, roomId)
	cancel()

	client.SendMsgOnlyOne(NewLikeMessageCount(roomId, currentLikeCount))

	client.SendMsgOnlyOne(NewOnlineCountMessage(roomId, onlineCount))

	// Broadcast join message
	h.manager.NoteJoin(roomId,username)

	// starts a new goroutine to send message to the client
	go client.WritePump()
	// read the data from client and block until the connection drops
	client.ReadPump(h.registry)

}
