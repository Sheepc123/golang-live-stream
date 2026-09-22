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

	// instant 为 true = 进出场和在线人数即时广播(实验 C 对照组)。
	instant bool
}

func NewWShandler(
	m *Manager,
	jwtcfg config.JWTConfig,
	registry *ActionRegistry,
	SessionMgr *live.SessionManager,
	instant bool,
) *WSHandler {
	return &WSHandler{
		manager:    m,
		jwtSecret:  jwtcfg.Secret,
		registry:   registry,
		SessionMgr: SessionMgr,
		instant:    instant,
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

	userId := claims.UserID
	username := claims.Username

	// 额度检查必须在 Upgrade 之前 —— 原因见 Manager.TryAcquireSlot。
	// 这里还是一个普通的 HTTP 请求,能正常回 503;
	// 一旦 Upgrade 成功,连接就变成了 WebSocket,再想「回一个状态码」
	// 只能先握手再发 Close 帧,该付的代价已经全付了。
	if !h.manager.TryAcquireSlot(userId) {
		logger.L().Warn("websocket rejected, connection limit reached",
			zap.Int64("room_id", roomId),
			zap.Int64("user_id", userId),
			zap.String("ip", c.ClientIP()),
		)
		response.Error(c, errno.TooManyConnections)
		return
	}
	// Upgrade 失败时这个 defer 也会跑,额度不会泄漏。
	defer h.manager.ReleaseSlot(userId)

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.L().Warn("websocket upgrade fail",
			zap.Int64("room_id", roomId),
			zap.Int64("user_id", userId),
			zap.String("ip", c.ClientIP()),
			zap.Error(err),
		)
		return
	}

	client := NewClient(roomId, userId, username, conn)

	defer func() {
		h.manager.Unregister(client)
		client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		n := h.SessionMgr.ViewerLeave(ctx, roomId, userId)

		if h.instant {
			// 对照组:每个人离开都广播两条。这条连接已经 Unregister,
			// 自己收不到,但房间里其他所有人都会收到 —— 这就是扇出。
			h.manager.BroadcastToRoom(roomId, NewOnlineCountMessage(roomId, n))
			h.manager.BroadcastToRoom(roomId, NewLeaveMessage(roomId, userId, username))
		} else {
			h.manager.NoteLeave(roomId, username)
		}

		if logger.DebugEnabled() {
			logger.L().Debug(
				"user left room",
				zap.Int64("room_id", roomId),
				zap.Int64("user_id", userId),
				zap.String("username", username),
			)
		}
	}()
	// Register Manager
	h.manager.Register(client)

	if logger.DebugEnabled() {
		logger.L().Debug(
			"user joined room",
			zap.Int64("room_id", roomId),
			zap.Int64("user_id", userId),
			zap.String("username", username),
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	onlineCount := h.SessionMgr.ViewerJoin(ctx, roomId, userId)

	currentLikeCount, _ := h.SessionMgr.GetLikeCount(ctx, roomId)
	cancel()

	client.SendMsgOnlyOne(NewLikeMessageCount(roomId, currentLikeCount))

	client.SendMsgOnlyOne(NewOnlineCountMessage(roomId, onlineCount))

	// Broadcast join message
	if h.instant {
		h.manager.BroadcastToRoom(roomId, NewOnlineCountMessage(roomId, onlineCount))
		h.manager.BroadcastToRoom(roomId, NewJoinMessage(roomId, userId, username))
	} else {
		// Broadcast join message
		h.manager.NoteJoin(roomId, username)
	}

	// starts a new goroutine to send message to the client
	go client.WritePump()
	// read the data from client and block until the connection drops
	client.ReadPump(h.registry)

}
