package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "live"

// ============ WebSocket ============

var (

	// WsConnections is the current number of active websocket connections.
	//
	// Gauge could increase and decrease.
	WSConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "ws",
		Name:      "connections",
		Help:      "Current number of active WebSocket connections",
	})

	// WSMessages the counts of message.
	//
	//	direction=up    Client → Servers ( User send messages/likes)
	//	direction=down  Serves-> Client  ( the total number of broadcast deliveries. fan-out count).
	//
	// 两者的比值就是「扇出比」:6 万人房间里,1 条上行产生 6 万次下行。
	// 压测报告里的「下行 QPS」就是 direction=down 这个数。
	WSMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "ws",
		Name:      "messages_total",
		Help:      "Total WebSocket messages by direction and type",
	}, []string{"direction", "type"})

	// WSDropped 主动丢弃的消息数。
	//
	//	reason=queue_full   BroadcastPool 队列满(全局背压)
	//	reason=client_slow  客户端 Send channel 满(单连接背压)
	//
	// 丢包是本项目刻意的设计(宁可丢弹幕,不可拖垮全局),
	// 但必须能量化 ——「极限负载下丢包率 < 1%」这句话
	// 就是 WSDropped / WSMessages{direction="down"} 算出来的。
	WSDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "ws",
		Name:      "dropped_total",
		Help:      "Total messages dropped by reason",
	}, []string{"reason"})

	// WSRejected 在 Upgrade 之前被拒绝的握手数。
	//
	//	reason=global_limit  全局连接数已达 server.max_ws_conns
	//	reason=user_limit    该用户并发连接数已达 server.max_conns_per_user
	//
	// 和 WSDropped 分开是因为两者的含义完全不同:
	// dropped 是「连上了但这条消息没送到」,rejected 是「根本没让你连」。
	// 压测时这条曲线开始上扬,说明已经撞到人为设的天花板,
	// 而不是撞到机器的极限 —— 分不清这两者,容量数据就是废的。
	WSRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "ws",
		Name:      "rejected_total",
		Help:      "Total WebSocket handshakes rejected before upgrade, by reason",
	}, []string{"reason"})

	// WSWriteBatch 每次 write 系统调用合并了多少条消息。
	// 这是 III.8 合并写的直接证据:低负载时 p50 应为 1,
	// 高负载时应明显大于 1。如果压测下仍是 1,说明合并没起作用。
	WSWriteBatch = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "ws",
		Name:      "write_batch_size",
		Help:      "Messages merged into one WebSocket frame per write",
		Buckets:   []float64{1, 2, 3, 4, 6, 8, 12, 16, 24, 32},
	})

	// BroadcastDuration 单次房间广播(deliver)的耗时分布。
	//
	// Bucket 刻意设得很小:纯内存操作应该在微秒级。
	// 如果 P99 跑到毫秒级,说明要么房间人数过大(热点房间),
	// 要么锁竞争严重 —— 这正是阶段 III 要解决的问题。
	BroadcastDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "broadcast_duration_seconds",
		Help:      "Duration of delivering one message to a whole room",
		Buckets: []float64{
			0.00001, 0.00005, 0.0001, 0.0005,
			0.001, 0.005, 0.01, 0.05, 0.1,
		},
	})
)

// ============ Kafka ============

var (
	// KafkaProduce 生产结果计数。result=ok|error
	KafkaProduce = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka",
		Name:      "produce_total",
		Help:      "Total Kafka produce attempts by result",
	}, []string{"result"})
)

// ============ Consumer ============

var (
	// ConsumerBatchSize 每批写库的条数分布。
	//
	// 阶段 III.5 会把逐条 INSERT 改成攒批,
	// 这个指标就是「批量是否真的生效」的直接证据:
	// 改造前恒为 1,改造后应该集中在 500 附近。
	ConsumerBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consumer",
		Name:      "batch_size",
		Help:      "Number of rows written to DB per batch",
		Buckets:   []float64{1, 10, 50, 100, 200, 500, 1000, 2000},
	})

	// ConsumerWriteDuration 每批落库耗时
	ConsumerWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consumer",
		Name:      "write_duration_seconds",
		Help:      "Duration of one batch DB write",
		Buckets:   prometheus.DefBuckets,
	})

	// ConsumerLag 消费延迟:落库时刻 - 弹幕发出时刻(秒)。
	//
	// 这个指标直接体现 Kafka 的「削峰」效果 ——
	// 平时接近 0,峰值时会涨到几秒,但消息不丢。
	ConsumerLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "consumer",
		Name:      "lag_seconds",
		Help:      "Delay between message sent time and DB write time",
		Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 300},
	})

	// ConsumerMessages 消费计数。result=ok|unmarshal_error|write_error
	ConsumerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "consumer",
		Name:      "messages_total",
		Help:      "Total consumed messages by result",
	}, []string{"result"})
)

// ============ 实验 B:同步写库 ============

var (
	// SyncDBWriteDuration 请求路径上单条 INSERT 的耗时。
	//
	// 和 ConsumerWriteDuration 放在一起看就是实验 B 的结论:
	// 同步写每条都付一次事务成本;攒批写 500 条付一次。
	SyncDBWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "sync_db",
		Name:      "write_duration_seconds",
		Help:      "Duration of one synchronous chat INSERT on the WS request path",
		Buckets:   prometheus.DefBuckets,
	})
)

// ============ HTTP ============

var (
	// HTTPRequests HTTP 请求计数。
	//
	// ⚠️ path 这个 label 必须用「路由模板」(/api/v1/rooms/:id),
	// 不能用「实际路径」(/api/v1/rooms/1、/rooms/2 ...),
	// 否则每个房间 ID 都会产生一条独立时间序列。
	// 具体见 middleware/metrics.go 里的 c.FullPath()。
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "http",
		Name:      "requests_total",
		Help:      "Total HTTP requests",
	}, []string{"method", "path", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "http",
		Name:      "duration_seconds",
		Help:      "HTTP request duration",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path"})
)
