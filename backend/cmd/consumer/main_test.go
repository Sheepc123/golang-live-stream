package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
)

// Embedded interfaces make unexpected calls fail instead of silently succeeding.
type batchRepoStub struct {
	repo.MsgRepo
	write func(context.Context, []entity.Message) error
}

func (r *batchRepoStub) CreateBatchIfAbsent(ctx context.Context, msgs []entity.Message) error {
	return r.write(ctx, msgs)
}

type sessionStub struct {
	sarama.ConsumerGroupSession
	mu    *sync.Mutex
	ctx   context.Context
	marks []int64
	steps *[]string
}

func (s *sessionStub) Context() context.Context { return s.ctx }
func (s *sessionStub) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marks = append(s.marks, msg.Offset)
	*s.steps = append(*s.steps, fmt.Sprintf("mark:%d", msg.Offset))
}

type claimStub struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c *claimStub) Topic() string                            { return "consumer-test" }
func (c *claimStub) Partition() int32                         { return 2 }
func (c *claimStub) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

type consumerHarness struct {
	mu      sync.Mutex
	claim   *claimStub
	session *sessionStub
	cancel  context.CancelFunc
	done    chan error
	writes  [][]entity.Message
	steps   []string
}

func startConsumer(t *testing.T, write func(context.Context) error) *consumerHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &consumerHarness{
		claim:  &claimStub{messages: make(chan *sarama.ConsumerMessage, consumerBatchSize+1)},
		cancel: cancel,
		done:   make(chan error, 1),
	}
	h.session = &sessionStub{ctx: ctx, steps: &h.steps, mu: &h.mu}
	handler := &consumerHandler{msgRepo: &batchRepoStub{
		write: func(ctx context.Context, msgs []entity.Message) error {
			// The consumer clears its backing array after success; retain a copy.
			h.mu.Lock()
			h.writes = append(h.writes, slices.Clone(msgs))
			h.steps = append(h.steps, "write")
			h.mu.Unlock()
			if write != nil {
				if err := write(ctx); err != nil {
					return err
				}
			}
			h.mu.Lock()
			h.steps = append(h.steps, "write-ok")
			h.mu.Unlock()
			return nil
		},
	}}
	go func() { h.done <- handler.ConsumeClaim(h.session, h.claim) }()
	// Also stop the consumer when an assertion terminates the test early.
	t.Cleanup(func() { cancel(); synctest.Wait() })
	return h
}

func (h *consumerHarness) send(t *testing.T, offset int64) {
	t.Helper()
	value, err := json.Marshal(ChatEvent{
		Type: "chat", RoomID: 7, UserID: 8, Username: "test-user",
		Content: fmt.Sprintf("message-%d", offset), Timestamp: time.Now().UnixMilli(), LiveSessionID: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.claim.messages <- &sarama.ConsumerMessage{Topic: h.claim.Topic(), Partition: 2, Offset: offset, Value: value}
}

func (h *consumerHarness) finish(t *testing.T, want error) {
	t.Helper()
	// Wait does not advance the virtual clock; the consumer must already exit.
	synctest.Wait()
	select {
	case err := <-h.done:
		if !errors.Is(err, want) {
			t.Fatalf("ConsumeClaim error = %v, want %v", err, want)
		}
	default:
		t.Fatal("ConsumeClaim did not exit")
	}
}

// Snapshot observations under a lock; advancing fake time is not a memory barrier.
func (h *consumerHarness) batches() [][]entity.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.writes)
}
func (h *consumerHarness) marked() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.session.marks)
}
func (h *consumerHarness) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.steps)
}

func TestConsumeClaimFlushesAtBatchLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startConsumer(t, nil)
		for i := int64(100); i < 599; i++ {
			h.send(t, i)
		}
		synctest.Wait()
		if len(h.batches()) != 0 || len(h.marked()) != 0 {
			t.Fatal("flushed before size or time limit")
		}
		h.send(t, 599)
		synctest.Wait()
		if len(h.batches()) != 1 || len(h.batches()[0]) != 500 {
			t.Fatalf("unexpected batches: %v", h.batches())
		}
		for i, msg := range h.batches()[0] {
			if msg.EventID != fmt.Sprintf("2-%d", 100+i) || msg.Content != fmt.Sprintf("message-%d", 100+i) {
				t.Fatalf("message %d lost its identity or order: %+v", i, msg)
			}
		}
		if !reflect.DeepEqual(h.events(), []string{"write", "write-ok", "mark:599"}) {
			t.Fatalf("steps: %v", h.events())
		}
		h.cancel()
		h.finish(t, nil)
	})
}

func TestConsumeClaimTimerFlushesAndReusesBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startConsumer(t, nil)
		h.send(t, 100)
		h.send(t, 101)
		synctest.Wait()
		time.Sleep(consumerFlushInterval - time.Nanosecond)
		synctest.Wait()
		if len(h.batches()) != 0 {
			t.Fatal("timer flushed too early")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if len(h.batches()) != 1 || len(h.batches()[0]) != 2 {
			t.Fatal("timer did not flush the partial batch")
		}
		h.send(t, 102)
		synctest.Wait()
		time.Sleep(consumerFlushInterval)
		synctest.Wait()
		if len(h.batches()) != 2 || len(h.batches()[1]) != 1 || h.batches()[1][0].EventID != "2-102" {
			t.Fatal("previous batch leaked into the next write")
		}
		time.Sleep(consumerFlushInterval)
		synctest.Wait()
		if len(h.batches()) != 2 {
			t.Fatal("empty batch caused a database write")
		}
		if !reflect.DeepEqual(h.marked(), []int64{101, 102}) {
			t.Fatalf("marks: %v", h.marked())
		}
		h.cancel()
		h.finish(t, nil)
	})
}

func TestConsumeClaimWriteFailureDoesNotMark(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writeErr := errors.New("injected database outage")
		h := startConsumer(t, func(context.Context) error { return writeErr })
		for i := int64(100); i < 600; i++ {
			h.send(t, i)
		}
		h.finish(t, writeErr)
		if len(h.batches()) != 1 || len(h.batches()[0]) != 500 {
			t.Fatal("expected one 500-message write attempt")
		}
		if len(h.marked()) != 0 {
			t.Fatalf("failed batch was marked: %v", h.marked())
		}
	})
}

func TestConsumeClaimMalformedMessageWaitsForEarlierWrite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_failure=%t", fail), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var writeErr error
				if fail {
					writeErr = errors.New("injected database outage")
				}
				h := startConsumer(t, func(context.Context) error { return writeErr })
				h.send(t, 100)
				h.send(t, 101)
				h.claim.messages <- &sarama.ConsumerMessage{Topic: h.claim.Topic(), Partition: 2, Offset: 102, Value: []byte("{broken")}
				synctest.Wait()
				if len(h.batches()) != 1 || len(h.batches()[0]) != 2 {
					t.Fatal("must flush only the earlier valid messages")
				}
				want := []string{"write"}
				if !fail {
					want = append(want, "write-ok", "mark:101", "mark:102")
					h.cancel()
				}
				if !reflect.DeepEqual(h.events(), want) {
					t.Fatalf("steps = %v, want %v", h.events(), want)
				}
				h.finish(t, writeErr)
			})
		})
	}
}

func TestConsumeClaimCancellationLeavesPendingBatchUnmarked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startConsumer(t, nil)
		h.send(t, 100)
		synctest.Wait()
		if len(h.claim.messages) != 0 {
			t.Fatal("message was not consumed into the pending batch")
		}
		h.cancel()
		h.finish(t, nil)
		if len(h.batches()) != 0 || len(h.marked()) != 0 {
			t.Fatal("cancelled session flushed or marked pending data")
		}
	})
}

func TestConsumeClaimClosedChannelFlushesRemainder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startConsumer(t, nil)
		h.send(t, 100)
		close(h.claim.messages)
		h.finish(t, nil)
		if !reflect.DeepEqual(h.events(), []string{"write", "write-ok", "mark:100"}) {
			t.Fatalf("steps: %v", h.events())
		}
	})
}

func TestConsumeClaimWriteContextExpiresWithoutMarking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := startConsumer(t, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		h.send(t, 100)
		synctest.Wait()
		time.Sleep(consumerFlushInterval + consumerWriteTimeout)
		h.finish(t, context.DeadlineExceeded)
		if len(h.batches()) != 1 || len(h.marked()) != 0 {
			t.Fatal("timed-out write was retried or marked")
		}
	})
}
