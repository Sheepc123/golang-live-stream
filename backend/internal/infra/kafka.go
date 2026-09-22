package infra

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/logger"
	"github.com/Sheepc123/golang-live-stream/internal/metrics"
	"go.uber.org/zap"
)

type KafkaProducer struct {
	producer sarama.AsyncProducer
	topic    string
}


//	NoResponse   (0): 什么都不等,发出即忘。broker 挂了都不知道。
//	WaitForLocal (1): leader 把数据写进「自己的 page cache」就返回。
//	WaitForAll  (-1): ISR 里所有副本都确认写进各自的 page cache。
func parseAcks(s string) (sarama.RequiredAcks, error) {
	switch s {
	case "all":
		return sarama.WaitForAll, nil
	case "local":
		return sarama.WaitForLocal, nil
	case "none":
		return sarama.NoResponse, nil
	default:
		// config 层的 oneof 已经拦过一遍,这里是第二道防线。
		// 两层校验不是重复:oneof 保证「配置文件里的值合法」,
		// 这里保证「代码里的映射没漏分支」—— 将来加了第四档而忘了
		// 改这个 switch,会在启动时立刻炸,而不是静默退化成默认值。
		return 0, fmt.Errorf("unknown kafka acks %q", s)
	}
}

// Keep runtime and integration checks on the same producer settings.
func newKafkaProducerConfig(cfg config.KafKaConfig) (*sarama.Config, error) {
	acks, err := parseAcks(cfg.Acks)
	if err != nil {
		return nil, err
	}

	sc := sarama.NewConfig()
	sc.Producer.RequiredAcks = acks

	// Hash Partitioner: Same key in the same paritition,
	// and Kafka Preserves message order within a parititon

	sc.Producer.Partitioner = sarama.NewHashPartitioner

	// Compress Kafka record batches; application payloads remain JSON.
	sc.Producer.Compression = sarama.CompressionLZ4

	// Best-effort broker-buffer flush triggers.
	// These are not per-room batch sizes or hard request-size limits.
	sc.Producer.Flush.Messages = 100
	sc.Producer.Flush.Frequency = 100 * time.Millisecond

	// Drain both result channels in the existing background goroutines.
	sc.Producer.Return.Errors = true
	sc.Producer.Return.Successes = true
	return sc, nil
}

func NewKafkaProducer(cfg config.KafKaConfig) (*KafkaProducer, error) {
	sc, err := newKafkaProducerConfig(cfg)
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewAsyncProducer(cfg.Brokers, sc)
	if err != nil {
		return nil, err
	}
	
	errCounter := metrics.KafkaProduce.WithLabelValues("error")
	okCounter := metrics.KafkaProduce.WithLabelValues("ok")

	go func() {
		for e := range p.Errors() {
			errCounter.Inc()
			logger.L().Error("kafka produce failed", zap.Error(e))
		}
	}()

	go func() {
		for range p.Successes() {
			okCounter.Inc()
		}
	}()

	return &KafkaProducer{producer: p, topic: cfg.Topic}, nil
}

func (k *KafkaProducer) Publish(PartitionKey string, value []byte) {
	k.producer.Input() <- &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   sarama.StringEncoder(PartitionKey),
		Value: sarama.ByteEncoder(value),
	}
}

func (k *KafkaProducer) Close() error {
	return k.producer.Close()
}
