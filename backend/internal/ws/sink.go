package ws

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"strconv"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/infra"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	"go.uber.org/zap"
)

// MsgSink 是「弹幕落库出口」的抽象。
//
// ChatAction 只关心一件事:这条弹幕要被持久化。至于是丢进 Kafka 让
// consumer 异步写,还是在请求路径上直接写 MySQL,它不该知道。
// 把这个决定抽成接口,实验 B(Kafka vs 同步写库)就只是 router 里
// 换一个实现,业务代码一行不动 —— 这是「依赖倒置」在本项目里最直接的用处。
type MsgSink interface {
	// Persist 尽力持久化一条弹幕。不返回 error:
	// 弹幕已经广播出去了,落库失败只能记日志和指标,没有可回滚的东西。
	Persist(msg Message)
}

// 编译期断言:两个实现都必须满足接口。少写一个方法就在这里报错,
// 而不是等到 router 注入时才发现。
var (
	_ MsgSink = (*KafkaSink)(nil)
	_ MsgSink = (*SyncDBSink)(nil)
)

// ---------- 生产路径:Kafka 异步 ----------

// KafkaSink 把弹幕投进 Kafka,由 cmd/consumer 攒批落库。
// 这段逻辑原来在 Manager.PersistMsg 里,原样搬过来。
type KafkaSink struct {
	producer *infra.KafkaProducer
}

func NewKafkaSink(p *infra.KafkaProducer) *KafkaSink {
	return &KafkaSink{producer: p}
}

func (k *KafkaSink) Persist(msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.L().Error("kafka sink marshal fail",
			zap.Int64("room_id", msg.RoomID),
			zap.Int64("user_id", msg.UserID),
			zap.Error(err),
		)
		return
	}

	// ── 分区键:RoomID ──
	//
	// 之前用 UserID,现在改成 RoomID。两个理由:
	//
	//  1. 落库局部性。同一房间的消息进同一分区,consumer 每批 500 条的
	//     room_id 高度集中。messages 表上 room_id 和 (live_session_id,
	//     sent_at) 都有索引,B+ 树插入点集中在少数几个页上 ——
	//     InnoDB 的二级索引维护成本直接下降。用 UserID 分区时,
	//     一批 500 条可能散落在几百个房间,每条都在不同的页上乱插。
	//
	//  2. 顺序保证已经不依赖分区了。sent_at 排序落地之后,
	//     历史消息的先后由 ORDER BY sent_at 决定,不再靠
	//     「同一分区内 offset 递增」。这是这次能改的前提 ——
	//     在 sent_at 之前动分区键会打乱同一用户的消息顺序。
	//
	// ⚠️ 单房间会成为分区热点(一个大主播独占一个分区)。
	// 这是刻意接受的:弹幕的瓶颈在下行扇出,不在 Kafka 写入,
	// 而落库的局部性收益是实打实的。
	k.producer.Publish(strconv.FormatInt(msg.RoomID, 10), data)
}

// ---------- 实验 B 对照组:请求路径上同步写 MySQL ----------

// 单条同步写的超时。比 consumer 的 3 秒短一点也无妨,
// 这里卡住的是用户的连接读循环,越短越能暴露问题。
const syncWriteTimeout = 3 * time.Second

// SyncDBSink 在 ReadPump 的 goroutine 里直接 INSERT。
//
// 注意它被调用的位置:ChatAction.Execute → Manager.PersistMsg → 这里。
// Execute 是在该连接的 ReadPump 里同步执行的,所以这次 INSERT 有多慢,
// 这个用户的下一条弹幕就要等多久。这正是实验 B 想量化的东西:
// 「没有 Kafka 削峰时,数据库延迟会直接传导到长连接上」。
type SyncDBSink struct {
	msgRepo repo.MsgRepo
}

func NewSyncDBSink(r repo.MsgRepo) *SyncDBSink {
	return &SyncDBSink{msgRepo: r}
}

func (s *SyncDBSink) Persist(msg Message) {
	row := entity.Message{
		// event_id 有唯一索引,不能为空。它的本意是 Kafka 重放去重,
		// 同步路径没有重放,只要保证唯一即可。
		// crypto/rand.Text() 是 Go 1.24 加的,返回 26 个字符的随机串,
		// 不用再引 uuid 库。
		EventID:       "sync-" + rand.Text(),
		RoomID:        msg.RoomID,
		UserID:        msg.UserID,
		Username:      msg.Username,
		Content:       msg.Content,
		Type:          msg.Type,
		LiveSessionID: msg.LiveSessionID,
		SentAt:        msg.Timestamp,
	}

	ctx, cancel := context.WithTimeout(context.Background(), syncWriteTimeout)
	defer cancel()

	start := time.Now()
	// 复用 consumer 用的同一个 repo 方法,单条也走 CreateInBatches,
	// SQL 形态一致,实验对比才公平。
	err := s.msgRepo.CreateBatchIfAbsent(ctx, []entity.Message{row})
	metrics.SyncDBWriteDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		logger.L().Error("sync db sink write fail",
			zap.Int64("room_id", msg.RoomID),
			zap.Int64("user_id", msg.UserID),
			zap.Error(err),
		)
	}
}
