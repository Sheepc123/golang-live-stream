package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"slices"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/IBM/sarama"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	"github.com/Sheepc123/golang-live-stream/internal/repo"
	drivermysql "github.com/go-sql-driver/mysql"
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

// wantEventID 生成期望的幂等键。集中在一处,免得三个测试各写一遍格式,
// 将来键格式再变时只改这里。
// claimStub 的 Topic() 是 "consumer-test"、Partition() 是 2。
func wantEventID(offset int64) string {
	return fmt.Sprintf("consumer-test-2-%d", offset)
}

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
			if msg.EventID != wantEventID(int64(100+i)) || msg.Content != fmt.Sprintf("message-%d", 100+i) {
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
		if len(h.batches()) != 2 || len(h.batches()[1]) != 1 || h.batches()[1][0].EventID != wantEventID(102) {
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
		writeErr := errors.New("unclassified write error")
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
					writeErr = errors.New("unclassified write error")
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
			// Every retry must get a live context with a new timeout budget.
			deadline, ok := ctx.Deadline()
			if ctx.Err() != nil || !ok || deadline.Sub(time.Now()) != consumerWriteTimeout {
				t.Error("write attempt reused an expired context or lost its timeout")
			}
			<-ctx.Done()
			return ctx.Err()
		})
		h.send(t, 100)
		synctest.Wait()
		time.Sleep(consumerFlushInterval + 3*consumerWriteTimeout + 3*consumerRetryBaseDelay)
		h.finish(t, context.DeadlineExceeded)
		if len(h.batches()) != 3 || len(h.marked()) != 0 {
			t.Fatal("unexpected timeout attempt count or marked unsuccessful batch")
		}
	})
}

func TestConsumeClaimRetrySucceedsBeforeMarking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := 0
		h := startConsumer(t, func(context.Context) error {
			attempt++
			if attempt == 1 {
				return driver.ErrBadConn
			}
			return nil
		})
		h.send(t, 100)
		h.send(t, 101)
		synctest.Wait()
		time.Sleep(consumerFlushInterval)
		synctest.Wait()
		if !reflect.DeepEqual(h.events(), []string{"write"}) {
			t.Fatalf("marked before successful write: %v", h.events())
		}
		time.Sleep(200*time.Millisecond - time.Nanosecond)
		synctest.Wait()
		if len(h.batches()) != 1 {
			t.Fatal("retried before the 200ms backoff elapsed")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !reflect.DeepEqual(h.events(), []string{"write", "write", "write-ok", "mark:101"}) {
			t.Fatalf("unexpected retry/mark order: %v", h.events())
		}
		batches := h.batches()
		if !reflect.DeepEqual(batches[0], batches[1]) {
			t.Fatal("retry changed the pending messages")
		}
		// A later message must start a fresh batch after successful recovery.
		h.send(t, 102)
		close(h.claim.messages)
		h.finish(t, nil)
		batches = h.batches()
		if len(batches) != 3 || len(batches[2]) != 1 || batches[2][0].EventID != wantEventID(102) {
			t.Fatal("recovered batch was not cleared before continuing consumption")
		}
	})
}

func TestConsumeClaimRetryExhaustionLeavesBatchUnmarked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writeErr := &drivermysql.MySQLError{Number: 1213, Message: "injected deadlock"}
		h := startConsumer(t, func(context.Context) error { return writeErr })
		h.send(t, 100)
		synctest.Wait()
		time.Sleep(consumerFlushInterval + 200*time.Millisecond)
		synctest.Wait()
		if len(h.batches()) != 2 || len(h.marked()) != 0 {
			t.Fatal("expected two failed attempts and no marks")
		}
		time.Sleep(400*time.Millisecond - time.Nanosecond)
		synctest.Wait()
		if len(h.batches()) != 2 {
			t.Fatal("third attempt did not wait 400ms")
		}
		time.Sleep(time.Nanosecond)
		h.finish(t, writeErr)
		if len(h.batches()) != 3 || len(h.marked()) != 0 {
			t.Fatal("retry exhaustion advanced progress or used the wrong attempt limit")
		}
	})
}

func TestConsumeClaimCancellationInterruptsRetry(t *testing.T) {
	for _, duringWrite := range []bool{false, true} {
		t.Run(fmt.Sprintf("during_write=%t", duringWrite), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := startConsumer(t, func(ctx context.Context) error {
					if duringWrite {
						<-ctx.Done()
					}
					// Even a retryable error must not outlive the session.
					return driver.ErrBadConn
				})
				h.send(t, 100)
				synctest.Wait()
				time.Sleep(consumerFlushInterval)
				synctest.Wait()
				h.cancel()
				h.finish(t, context.Canceled)
				if len(h.batches()) != 1 || len(h.marked()) != 0 {
					t.Fatal("cancelled session retried or marked pending data")
				}
			})
		})
	}
}

func TestConsumeClaimInvalidDataIsNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writeErr := &drivermysql.MySQLError{Number: 1406, Message: "data too long"}
		h := startConsumer(t, func(context.Context) error { return fmt.Errorf("insert batch: %w", writeErr) })
		h.send(t, 100)
		close(h.claim.messages)
		h.finish(t, writeErr)
		if len(h.batches()) != 1 || len(h.marked()) != 0 {
			t.Fatal("invalid data was retried or marked")
		}
	})
}

func TestIsRetryableWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"cancelled", context.Canceled, false},
		{"write_timeout", context.DeadlineExceeded, true},
		{"bad_connection", driver.ErrBadConn, true},
		{"invalid_connection", drivermysql.ErrInvalidConn, true},
		{"eof", io.EOF, true},
		{"unexpected_eof", io.ErrUnexpectedEOF, true},
		{"network", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"lock_timeout", &drivermysql.MySQLError{Number: 1205}, true},
		{"deadlock", &drivermysql.MySQLError{Number: 1213}, true},
		{"invalid_data", &drivermysql.MySQLError{Number: 1406}, false},
		{"authentication", &drivermysql.MySQLError{Number: 1045}, false},
		{"unknown", errors.New("unclassified failure"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableWriteError(tc.err); got != tc.want {
				t.Fatalf("got %t, want %t", got, tc.want)
			}
			if tc.err != nil {
				if got := isRetryableWriteError(fmt.Errorf("repository: %w", tc.err)); got != tc.want {
					t.Fatalf("wrapped error: got %t, want %t", got, tc.want)
				}
			}
		})
	}
}
