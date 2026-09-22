package infra

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/config"
	gometrics "github.com/rcrowley/go-metrics"
)

// These checks use disposable topics, never the configured application topic.
// Run through scripts/test-kafka-producer.sh against a development Kafka cluster.
type kafkaFixture struct {
	brokers []string
	admin   sarama.ClusterAdmin
}

func newKafkaFixture(t *testing.T) *kafkaFixture {
	t.Helper()
	if os.Getenv("RUN_KAFKA_INTEGRATION") != "1" {
		t.Skip("run scripts/test-kafka-producer.sh to test a real Kafka broker")
	}
	address := os.Getenv("KAFKA_TEST_BROKERS")
	if address == "" {
		address = "localhost:9092"
	}
	brokers := strings.Split(address, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	admin, err := sarama.NewClusterAdmin(brokers, kafkaCheckConfig())
	if err != nil {
		t.Fatalf("connect to test Kafka: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close Kafka admin: %v", err)
		}
	})
	return &kafkaFixture{brokers: brokers, admin: admin}
}

func kafkaCheckConfig() *sarama.Config {
	c := sarama.NewConfig()
	c.Net.DialTimeout = 5 * time.Second
	c.Net.ReadTimeout = 5 * time.Second
	c.Net.WriteTimeout = 5 * time.Second
	c.Admin.Timeout = 5 * time.Second
	c.Metadata.Retry.Max = 2
	c.Consumer.Return.Errors = true
	return c
}

func (f *kafkaFixture) topic(t *testing.T) string {
	t.Helper()
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "producer-check-" + hex.EncodeToString(suffix[:])
	retention := "600000"
	err := f.admin.CreateTopic(name, &sarama.TopicDetail{
		NumPartitions: 3, ReplicationFactor: 1,
		ConfigEntries: map[string]*string{"retention.ms": &retention},
	}, false)
	if err != nil {
		t.Fatalf("create disposable topic: %v", err)
	}
	t.Logf("disposable topic: %s", name)
	t.Cleanup(func() {
		if err := f.admin.DeleteTopic(name); err != nil {
			t.Errorf("delete disposable topic %s: %v", name, err)
		}
	})
	return name
}

type kafkaTestMessage struct {
	key   string
	value []byte
}

func isKafkaMetadataTransition(err error) bool {
	return errors.Is(err, sarama.ErrNotLeaderForPartition) ||
		errors.Is(err, sarama.ErrLeaderNotAvailable) ||
		errors.Is(err, sarama.ErrUnknownTopicOrPartition)
}

func kafkaTestMessages(t *testing.T, count int) []kafkaTestMessage {
	t.Helper()
	msgs := make([]kafkaTestMessage, count)
	for i := range msgs {
		userID := int64(i%20 + 1)
		// Fixed input across profiles: UTF-8 JSON with unique message content.
		value, err := json.Marshal(struct {
			Type          string `json:"type"`
			RoomID        int64  `json:"room_id"`
			UserID        int64  `json:"user_id"`
			Username      string `json:"username"`
			Content       string `json:"content"`
			Timestamp     int64  `json:"timestamp"`
			LiveSessionID int64  `json:"live_session_id"`
		}{"chat", int64(i%3 + 1), userID, fmt.Sprintf("user-%02d", userID),
			fmt.Sprintf("message-%06d 直播测试 %s", i, strings.Repeat("hello Kafka ", 8)), 1700000000000, 1})
		if err != nil {
			t.Fatal(err)
		}
		msgs[i] = kafkaTestMessage{key: strconv.FormatInt(userID, 10), value: value}
	}
	return msgs
}

// Consume each partition from its beginning. Compare every payload and key,
// reject duplicate records, and verify there are no extra records in the log.
func (f *kafkaFixture) verify(t *testing.T, topic string, msgs []kafkaTestMessage) {
	t.Helper()
	client, err := sarama.NewClient(f.brokers, kafkaCheckConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		t.Fatal(err)
	}
	type received struct {
		msg *sarama.ConsumerMessage
		err error
	}
	incoming := make(chan received, len(msgs)+len(partitions))
	stop := make(chan struct{})
	defer close(stop)
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		// A newly created topic can briefly advertise a leader before its
		// partition is ready. Retry only metadata-transition errors, with a cap.
		until := time.Now().Add(5 * time.Second)
		for isKafkaMetadataTransition(err) && time.Now().Before(until) {
			t.Logf("waiting for test partition %d metadata: %v", partition, err)
			time.Sleep(100 * time.Millisecond)
			if refreshErr := client.RefreshMetadata(topic); refreshErr != nil {
				t.Fatal(refreshErr)
			}
			pc, err = consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		go func(pc sarama.PartitionConsumer) {
			for {
				var result received
				select {
				case <-stop:
					return
				case msg, ok := <-pc.Messages():
					if !ok {
						return
					}
					result.msg = msg
				case err, ok := <-pc.Errors():
					if !ok {
						return
					}
					result.err = err
				}
				select {
				case incoming <- result:
				case <-stop:
					return
				}
			}
		}(pc)
	}
	expected := make(map[string]string, len(msgs))
	for _, msg := range msgs {
		expected[string(msg.value)] = msg.key
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for len(expected) > 0 {
		select {
		case result := <-incoming:
			if result.err != nil {
				// Sarama refreshes the partition leader automatically. The
				// verification deadline still fails if recovery never completes.
				if isKafkaMetadataTransition(result.err) {
					t.Logf("test reader is refreshing metadata: %v", result.err)
					continue
				}
				t.Fatal(result.err)
			}
			key, ok := expected[string(result.msg.Value)]
			if !ok {
				t.Fatal("unexpected or duplicate Kafka payload")
			}
			if !bytes.Equal(result.msg.Key, []byte(key)) {
				t.Fatal("Kafka message key changed")
			}
			delete(expected, string(result.msg.Value))
		case <-timer.C:
			t.Fatalf("timed out with %d messages still missing", len(expected))
		}
	}
	var total int64
	for _, partition := range partitions {
		end, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			t.Fatal(err)
		}
		total += end // Fresh, non-transactional topics start at offset zero.
	}
	if total != int64(len(msgs)) {
		t.Fatalf("Kafka contains %d records, want %d", total, len(msgs))
	}
}

func TestKafkaProducerDelivery(t *testing.T) {
	f := newKafkaFixture(t)
	for _, tc := range []struct {
		name       string
		count      int
		closeFirst bool
	}{
		{"one_message_without_close", 1, false},
		{"continuous_messages", 500, false},
		{"close_flushes_partial_batch", 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := f.topic(t)
			p, err := NewKafkaProducer(config.KafKaConfig{
				Brokers: f.brokers,
				Topic:   topic,
				Acks:    "all", 
			})
			
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := p.Close(); err != nil {
						t.Errorf("close producer: %v", err)
					}
				}
			})
			msgs := kafkaTestMessages(t, tc.count)
			for _, msg := range msgs {
				p.Publish(msg.key, msg.value)
			}
			if tc.closeFirst {
				err := p.Close()
				closed = true
				if err != nil {
					t.Fatal(err)
				}
			}
			// In the first two cases, delivery must work without closing first.
			f.verify(t, topic, msgs)
			t.Logf("verified %d exact JSON payloads and keys", len(msgs))
		})
	}
}

type kafkaObservation struct {
	Profile                string  `json:"profile"`
	Workload               string  `json:"workload"`
	Messages               int     `json:"messages"`
	OfferedIntervalMS      float64 `json:"offered_interval_ms"`
	PayloadBytes           int     `json:"payload_bytes"`
	OutgoingBytes          int64   `json:"client_outgoing_bytes"`
	NetworkRequests        int64   `json:"client_network_requests"`
	EncodedProduceRequests int64   `json:"encoded_produce_requests"`
	AckP50MS               float64 `json:"ack_p50_ms"`
	AckP95MS               float64 `json:"ack_p95_ms"`
	AckP99MS               float64 `json:"ack_p99_ms"`
	ElapsedMS              float64 `json:"elapsed_ms"`
}

// These are observations, not performance assertions: shared broker load,
// cold metadata requests, and workload shape influence the results.
func TestKafkaProducerComparison(t *testing.T) {
	f := newKafkaFixture(t)
	for _, workload := range []struct {
		name     string
		count    int
		interval time.Duration
	}{
		{"low", 5, 200 * time.Millisecond},
		{"steady", 1000, time.Millisecond},
	} {
		msgs := kafkaTestMessages(t, workload.count)
		for _, profile := range []string{"baseline", "lz4", "lz4_batch"} {
			t.Run(workload.name+"/"+profile, func(t *testing.T) {
				topic := f.topic(t)
				cfg, err := newKafkaProducerConfig(config.KafKaConfig{Acks: "all"})
				if err != nil {
					t.Fatal(err)
				}
				cfg.MetricRegistry = gometrics.NewRegistry()
				if profile != "lz4_batch" {
					cfg.Producer.Flush.Messages = 0
					cfg.Producer.Flush.Frequency = 0
				}
				if profile == "baseline" {
					cfg.Producer.Compression = sarama.CompressionNone
				}
				p, err := sarama.NewAsyncProducer(f.brokers, cfg)
				if err != nil {
					t.Fatal(err)
				}
				// Metadata carries enqueue time only in memory, not in Kafka values.
				acks := make(chan time.Duration, len(msgs))
				failures := make(chan error, len(msgs))
				drained := make(chan struct{})
				go func() {
					defer close(drained)
					successes, errs := p.Successes(), p.Errors()
					for successes != nil || errs != nil {
						select {
						case msg, ok := <-successes:
							if !ok {
								successes = nil
								continue
							}
							acks <- time.Since(msg.Metadata.(time.Time))
						case err, ok := <-errs:
							if !ok {
								errs = nil
								continue
							}
							failures <- err
						}
					}
				}()
				t.Cleanup(func() {
					if err := p.Close(); err != nil {
						t.Errorf("close producer: %v", err)
					}
					<-drained
				})
				result := kafkaObservation{Profile: profile, Workload: workload.name, Messages: len(msgs), OfferedIntervalMS: float64(workload.interval) / float64(time.Millisecond)}
				start := time.Now()
				deadline := time.NewTimer(20 * time.Second)
				defer deadline.Stop()
				for i, msg := range msgs {
					// Schedule the same offered rate for each profile. A slow input
					// channel is included in latency; this is not a capacity benchmark.
					if wait := time.Until(start.Add(time.Duration(i) * workload.interval)); wait > 0 {
						time.Sleep(wait)
					}
					out := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(msg.key), Value: sarama.ByteEncoder(msg.value), Metadata: time.Now()}
					select {
					case p.Input() <- out:
						result.PayloadBytes += len(msg.value)
					case err := <-failures:
						t.Fatal(err)
					case <-deadline.C:
						t.Fatal("producer input timed out")
					}
				}
				latencies := make([]float64, 0, len(msgs))
				for len(latencies) < len(msgs) {
					select {
					case elapsed := <-acks:
						latencies = append(latencies, float64(elapsed)/float64(time.Millisecond))
					case err := <-failures:
						t.Fatal(err)
					case <-deadline.C:
						t.Fatal("Kafka acknowledgement timed out")
					}
				}
				result.ElapsedMS = float64(time.Since(start)) / float64(time.Millisecond)
				// Snapshot before Close unregisters the broker's metrics. Counts
				// include this producer's metadata/protocol traffic, not just payload.
				if m, ok := cfg.MetricRegistry.Get("outgoing-byte-rate").(gometrics.Meter); ok {
					result.OutgoingBytes = m.Count()
				}
				if m, ok := cfg.MetricRegistry.Get("request-rate").(gometrics.Meter); ok {
					result.NetworkRequests = m.Count()
				}
				if h, ok := cfg.MetricRegistry.Get("records-per-request").(gometrics.Histogram); ok {
					result.EncodedProduceRequests = h.Count()
				}
				if result.OutgoingBytes == 0 || result.EncodedProduceRequests == 0 {
					t.Fatal("expected Sarama metrics are unavailable")
				}
				sort.Float64s(latencies)
				percentile := func(p float64) float64 { return latencies[int(math.Ceil(p*float64(len(latencies))))-1] }
				result.AckP50MS, result.AckP95MS, result.AckP99MS = percentile(.50), percentile(.95), percentile(.99)
				f.verify(t, topic, msgs)
				encoded, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("KAFKA_RESULT %s", encoded)
			})
		}
	}
}
