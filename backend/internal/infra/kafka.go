package infra

import (
	"log"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/config"
)

type KafkaProducer struct {
	producer sarama.AsyncProducer
	topic    string
}

func NewKafkaProducer(cfg config.KafKaConfig) (*KafkaProducer, error) {
	sc := sarama.NewConfig()

	// wait For acknowledgements from the leader and all followers.
	sc.Producer.RequiredAcks = sarama.WaitForAll

	// Hash Partitioner: Same key in the same paritition,
	// and Kafka Preserves message order within a parititon

	sc.Producer.Partitioner = sarama.NewHashPartitioner

	sc.Producer.Return.Errors = true

	p, err := sarama.NewAsyncProducer(cfg.Brokers, sc)

	if err != nil {
		return nil, err
	}

	go func() {
		for e := range p.Errors() {
			log.Printf("failed to send message to Kafka : %v", e)
		}
	}()

	return &KafkaProducer{producer: p, topic: cfg.Topic}, nil

}

func (k *KafkaProducer) Publish(roomId int64, value []byte) {
	k.producer.Input() <- &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   sarama.StringEncoder(strconv.FormatInt(roomId, 10)),
		Value: sarama.ByteEncoder(value),
	}
}
func (k *KafkaProducer) Close() error {
	return k.producer.Close()
}
