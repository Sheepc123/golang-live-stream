package ws

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/errno"
	"github.com/Sheepc123/golang-live-stream/internal/live"
	"github.com/Sheepc123/golang-live-stream/internal/response"
	Jwttoken "github.com/Sheepc123/golang-live-stream/internal/token"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
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
		response.Fail(c, 401, errno.Unauthorized.Code, errno.Unauthorized.Msg, gin.H{})
		return
	}

	// get userId and Username through token
	claims, err := Jwttoken.ParseAccessToken(accessToken, h.jwtSecret)
	if err != nil {
		response.Fail(c, 401, errno.Unauthorized.Code, errno.Unauthorized.Msg, gin.H{})
		return
	}
	//Get roomId through url
	roomId, err := strconv.ParseInt(c.Param("room_id"), 10, 64)
	if err != nil {
		response.Fail(c, 400, errno.WSInvalidRoomID.Code, errno.WSInvalidRoomID.Msg, gin.H{})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket upgrade fail: %v", err)
		return
	}

	userId := claims.UserID
	username := claims.Username
	client := NewClient(roomId, userId, username, conn)

	if err != nil {
		log.Printf("resolve session on connect fail (room=%d): %v", roomId, err)
	}

	h.manager.TrackConn()
	defer h.manager.UnTrackConn()

	defer func() {
		h.manager.Unregister(client)
		client.Close()

		leaveMSg := NewLeaveMessage(userId, roomId, username)
		h.manager.BroadcastToRoom(roomId, leaveMSg)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		remaining := h.SessionMgr.ViewerLeave(ctx, roomId, userId)
		cancel()

		OnlineCountMsg := NewOnlineCountMessage(roomId, remaining)
		h.manager.BroadcastToRoom(roomId, OnlineCountMsg)

		log.Printf("User left room: room_id = %d, user_id = %d, username = %s", roomId, userId, username)

	}()
	// Register Manager
	h.manager.Register(client)

	log.Printf("User join the live stream room: room_id = %d,user_id = %d,username = %v", roomId, userId, username)

	ctx, Cancel := context.WithTimeout(context.Background(), 3*time.Second)
	onlineCount := h.SessionMgr.OnlineCount(ctx, roomId)
	currentLikeCount := h.SessionMgr.GetLikeCount(ctx, roomId)
	Cancel()

	likeCountMsg := NewLikeMessageCount(roomId, currentLikeCount)
	client.Send <- likeCountMsg

	// Broadcast join message
	joinMsg := NewJoinMessage(userId, roomId, username)
	h.manager.BroadcastToRoom(joinMsg.RoomID, joinMsg)

	// broadceast online count message
	countMsg := NewOnlineCountMessage(roomId, onlineCount)
	h.manager.BroadcastToRoom(countMsg.RoomID, countMsg)

	// starts a new goroutine to send message to the client
	go client.WritePump()
	// read the data from client and block until the connection drops
	client.ReadPump(h.registry)

}
