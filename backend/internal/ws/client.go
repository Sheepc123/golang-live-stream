package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait = 3 * time.Second

	//PongWait represents the Server send msg to client max waiting time
	pongWait   = 10 * time.Second
	pingPeriod = (pongWait * 9) / 10

	maxMessageSize = 2048
	sendBuffer     = 32
)

// newline 是合并帧里多条 JSON 之间的分隔符(NDJSON)。
var newline = []byte{'\n'}

// 上行限流配额。两种动作分开算,因为成本和用户预期都不一样。
const (
	// 弹幕:每秒 2 条,允许突发 5 条。
	// 真人手动打字的上限大约就是这个量级,超过基本可以断定是脚本。
	// 单条弹幕的成本是「一次广播扇出 + 一次落库」,是全项目最贵的上行动作。
	chatRate, chatBurst = 2, 5

	// 点赞:配额比弹幕宽得多。
	// 它是「连点」型交互(用户会疯狂戳心),没有内容也不入库,
	// 单次成本只有一次 Redis INCR。
	// 但也必须有上限 —— 不限的话一条连接就能按线速打满 Redis。
	likeRate, likeBurst = 10, 20
)

// Client represents a user connected to a live room through websocket.
type Client struct {
	RoomID   int64
	UserID   int64
	Username string
	Conn     *websocket.Conn

	// send is a buffer channel saving slice header
	// readOnly
	Send      chan []byte
	closeOnce sync.Once

	// chatLimiter / likeLimiter 限制这条连接的上行速率。
	//
	// 是值而不是指针、也不带锁:它们只被本连接的 ReadPump goroutine
	// 访问(Dispatch → Action.Execute 都在那个栈上)。原因见 ratelimit.go。
	chatLimiter tokenBucket
	likeLimiter tokenBucket
}

func NewClient(roomID int64, userId int64, username string, conn *websocket.Conn) *Client {
	now := time.Now()
	return &Client{
		RoomID:      roomID,
		UserID:      userId,
		Conn:        conn,
		Username:    username,
		Send:        make(chan []byte, sendBuffer),
		chatLimiter: newTokenBucket(chatRate, chatBurst, now),
		likeLimiter: newTokenBucket(likeRate, likeBurst, now),
	}
}

// ReadPump read the message from the frontend.
// dispatch msg through ActionRegistry
func (c *Client) ReadPump(registry *ActionRegistry) {

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))

	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var msg Message

		err := c.Conn.ReadJSON(&msg)

		if err != nil {
			if logger.DebugEnabled() {
				logger.L().Debug("read pump stopped",
					zap.Int64("user_id", c.UserID),
					zap.Int64("room_id", c.RoomID),
					zap.Error(err),
				)
			}
			return
		}

		c.Conn.SetReadDeadline(time.Now().Add(pongWait))

		registry.Dispatch(c, msg)
	}
}

// WritePump write the message from backend to the frontend
func (c *Client) WritePump() {

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {

		case payload, ok := <-c.Send:
			if !ok {
				c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// start 同时用于写超时和写耗时指标,不额外多调一次 time.Now。
			start := time.Now()
			c.Conn.SetWriteDeadline(start.Add(writeWait))

			// ── 合并写:把通道里当前积压的消息塞进同一个 WebSocket 帧 ──
			//
			// 压测火焰图显示 55% 的 CPU 在 write 系统调用上,每条消息一次。
			// NextWriter 拿到一个帧的写入器,往里写多条、用 '\n' 分隔,
			// Close 时才真正发出 —— N 条消息一次 syscall。
			// (前提是这一帧不超过写缓冲 WriteBufferSize,见 metrics.WSWriteBytes。)
			//
			// 不加定时器攒批:低负载时通道里只有这一条,立刻发,零额外延迟;
			// 高负载时 WritePump 每次被调度到,通道里已经堆了好几条,
			// 合并比例随拥塞自动上升。这是「自适应批处理」,不用调参数。
			//
			// 上限 maxBatch 防止一个帧无限长:通道容量是 32,
			// 所以一帧最多 32 条,和 sendBuffer 对齐即可。
			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(payload)
			n, size := 1, len(payload)
		drain:
			for n < sendBuffer {
				select {
				case more, ok := <-c.Send:
					if !ok {
						break drain // 通道关了,先把手上的发完,下一轮再处理 Close
					}
					w.Write(newline)
					w.Write(more)
					n++
					size += 1 + len(more)
				default:
					break drain // 通道空了,不等
				}
			}
			if err := w.Close(); err != nil {
				if logger.DebugEnabled() {
					logger.L().Debug("write pump stopped",
						zap.Int64("user_id", c.UserID),
						zap.Int64("room_id", c.RoomID),
						zap.Int("batch", n),
						zap.Error(err),
					)
				}
				return
			}
			metrics.WSWriteDuration.Observe(time.Since(start).Seconds())
			metrics.WSWriteBatch.Observe(float64(n))
			metrics.WSWriteBytes.Add(float64(size))

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))

			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				if logger.DebugEnabled() {
					logger.L().Debug("ping failed, closing",
						zap.Int64("user_id", c.UserID),
						zap.Error(err),
					)
				}
				return
			}
		}
	}
}

// Send Message only one person
// without Redis and broadcast pool.
func (c *Client) SendMsgOnlyOne(msg Message) bool {
	payload, err := json.Marshal(msg)

	if err != nil {
		logger.L().Error("encode direct message fail",
			zap.String("type", msg.Type),
			zap.Int64("user_id", c.UserID),
			zap.Error(err),
		)
		return false
	}

	select {
	case c.Send <- payload:
		metrics.WSMessages.WithLabelValues("down", msg.Type).Inc()
		return true
	default:
		metrics.WSDropped.WithLabelValues("client_slow").Inc()
		return false
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		err := c.Conn.Close()
		if err != nil && logger.DebugEnabled() {
			logger.L().Debug("close websocket fail",
				zap.Int64("user_id", c.UserID),
				zap.Error(err),
			)
		}
	})
}
