package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// open builds a manager backed by a fresh temporary log.
func open(t *testing.T, cfg Config) (*Manager, *Queue, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")
	mgr, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })

	q, err := mgr.Create(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return mgr, q, path
}

// drain receives up to n messages and returns their bodies in delivery order.
func drain(t *testing.T, q *Queue, n int) []string {
	t.Helper()
	msgs, err := q.Receive(context.Background(), n, time.Minute, 0)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Body)
	}
	return out
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// --- ordering: the four combinations the brief asks for ---

func TestFIFO(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	for _, b := range []string{"a", "b", "c"} {
		if _, err := q.Enqueue(b, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, drain(t, q, 3), []string{"a", "b", "c"})
}

func TestLIFO(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: LIFO})
	for _, b := range []string{"a", "b", "c"} {
		if _, err := q.Enqueue(b, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, drain(t, q, 3), []string{"c", "b", "a"})
}

// Priority outranks arrival order, and arrival order still decides ties. This
// is the composite key doing its job.
func TestPriorityFIFO(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO, Priority: true})
	q.Enqueue("low-first", 1, 0)
	q.Enqueue("high-first", 9, 0)
	q.Enqueue("low-second", 1, 0)
	q.Enqueue("high-second", 9, 0)

	eq(t, drain(t, q, 4), []string{"high-first", "high-second", "low-first", "low-second"})
}

func TestPriorityLIFO(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: LIFO, Priority: true})
	q.Enqueue("low-first", 1, 0)
	q.Enqueue("high-first", 9, 0)
	q.Enqueue("low-second", 1, 0)
	q.Enqueue("high-second", 9, 0)

	// Priority still wins; within a priority the newest arrival goes first.
	eq(t, drain(t, q, 4), []string{"high-second", "high-first", "low-second", "low-first"})
}

func TestPriorityRejectedWhenDisabled(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO, Priority: false})
	if _, err := q.Enqueue("x", 5, 0); err != ErrPriorityDisabled {
		t.Fatalf("want ErrPriorityDisabled, got %v", err)
	}
}

// --- delay: the orthogonal axis ---

func TestDelayGate(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("later", 0, 120*time.Millisecond)
	q.Enqueue("now", 0, 0)

	// The delayed message is invisible even though it was enqueued first.
	eq(t, drain(t, q, 10), []string{"now"})

	time.Sleep(200 * time.Millisecond)
	eq(t, drain(t, q, 10), []string{"later"})
}

// The whole point of the brief: delay, priority and LIFO composing at once.
func TestDelayedPriorityLIFO(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: LIFO, Priority: true})
	q.Enqueue("delayed-high", 9, 100*time.Millisecond)
	q.Enqueue("ready-low-first", 1, 0)
	q.Enqueue("ready-low-second", 1, 0)

	// High priority does not help a message that is not yet eligible.
	eq(t, drain(t, q, 10), []string{"ready-low-second", "ready-low-first"})

	time.Sleep(180 * time.Millisecond)
	eq(t, drain(t, q, 10), []string{"delayed-high"})
}

// --- leases ---

func TestLeaseExpiryRedelivers(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("work", 0, 0)

	first, err := q.Receive(context.Background(), 1, 80*time.Millisecond, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("first receive: %v %v", first, err)
	}
	if first[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", first[0].Attempts)
	}

	// While the lease is live nobody else can have it.
	if got := drain(t, q, 1); len(got) != 0 {
		t.Fatalf("message leaked while leased: %v", got)
	}

	// The consumer never acked, so the message must come back.
	time.Sleep(150 * time.Millisecond)
	second, _ := q.Receive(context.Background(), 1, time.Minute, 0)
	if len(second) != 1 {
		t.Fatal("message was not redelivered after its lease expired")
	}
	if second[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", second[0].Attempts)
	}
}

func TestAckRemovesPermanently(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("work", 0, 0)

	msgs, _ := q.Receive(context.Background(), 1, 50*time.Millisecond, 0)
	if err := q.Ack(msgs[0].Receipt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond) // well past the lease
	if got := drain(t, q, 1); len(got) != 0 {
		t.Fatalf("acked message came back: %v", got)
	}
}

func TestStaleReceiptRejected(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("work", 0, 0)

	msgs, _ := q.Receive(context.Background(), 1, 50*time.Millisecond, 0)
	stale := msgs[0].Receipt

	time.Sleep(120 * time.Millisecond)
	q.Tick(time.Now()) // lease expires, message returns to ready

	if err := q.Ack(stale); err != ErrNoLease {
		t.Fatalf("stale receipt was accepted, got %v", err)
	}
}

func TestNackRequeuesWithBackoff(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("work", 0, 0)

	msgs, _ := q.Receive(context.Background(), 1, time.Minute, 0)
	if err := q.Nack(msgs[0].Receipt, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := drain(t, q, 1); len(got) != 0 {
		t.Fatal("nacked message ignored its backoff")
	}
	time.Sleep(160 * time.Millisecond)
	if got := drain(t, q, 1); len(got) != 1 {
		t.Fatal("nacked message never came back")
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO, MaxAttempts: 2})
	q.Enqueue("poison", 0, 0)

	for i := 0; i < 2; i++ {
		msgs, _ := q.Receive(context.Background(), 1, time.Minute, 0)
		if len(msgs) != 1 {
			t.Fatalf("attempt %d: no message", i+1)
		}
		q.Nack(msgs[0].Receipt, 0)
	}

	if s := q.Stats(); s.DLQ != 1 || s.Ready != 0 {
		t.Fatalf("want 1 in dlq and 0 ready, got dlq=%d ready=%d", s.DLQ, s.Ready)
	}

	if n := q.ReplayDLQ(); n != 1 {
		t.Fatalf("replayed %d, want 1", n)
	}
	if got := drain(t, q, 1); len(got) != 1 {
		t.Fatal("replayed message did not return to ready")
	}
}

// --- durability: the hard requirement ---

func TestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	q, err := mgr.Create(Config{Name: "orders", Mode: LIFO, Priority: true, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue("low", 1, 0)
	q.Enqueue("high", 9, 0)
	q.Enqueue("acked-away", 5, 0)

	msgs, _ := q.Receive(context.Background(), 1, time.Minute, 0)
	if msgs[0].Body != "high" {
		t.Fatalf("expected priority order before restart, got %q", msgs[0].Body)
	}
	// Leave "high" leased and unacked. It must come back after the restart.
	m2, _ := q.Receive(context.Background(), 1, time.Minute, 0)
	if m2[0].Body != "acked-away" {
		t.Fatalf("got %q", m2[0].Body)
	}
	q.Ack(m2[0].Receipt)

	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart from nothing but the log on disk.
	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr2.Close()

	q2, err := mgr2.Get("orders")
	if err != nil {
		t.Fatalf("queue did not survive restart: %v", err)
	}
	if cfg := q2.Config(); cfg.Mode != LIFO || !cfg.Priority || cfg.MaxAttempts != 3 {
		t.Fatalf("config did not survive restart: %+v", cfg)
	}

	// The acked message is gone; the leased one is back and still ordered.
	eq(t, drain(t, q2, 10), []string{"high", "low"})
}

// A crash midway through a write leaves a partial record. Recovery must drop
// that tail and keep everything before it, rather than refusing to start.
func TestTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, _ := Open(path, 0, 10*time.Millisecond)
	q, _ := mgr.Create(Config{Name: "q", Mode: FIFO})
	q.Enqueue("first", 0, 0)
	q.Enqueue("second", 0, 0)
	mgr.Close()

	// Simulate the process dying with a half written record on the end.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x00, 0x00, 0x01, 0x2c, 0xde, 0xad, 0xbe, 0xef, 'j', 'u', 'n', 'k'})
	f.Close()

	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("recovery refused to start on a torn tail: %v", err)
	}
	defer mgr2.Close()

	q2, err := mgr2.Get("q")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, drain(t, q2, 10), []string{"first", "second"})

	// The garbage must be gone, otherwise the next append lands after
	// unreadable bytes and the log is permanently unrecoverable.
	if _, err := q2.Enqueue("third", 0, 0); err != nil {
		t.Fatal(err)
	}
	mgr2.Close()

	mgr3, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("log was not usable after truncation: %v", err)
	}
	defer mgr3.Close()
	q3, _ := mgr3.Get("q")
	eq(t, drain(t, q3, 10), []string{"first", "second", "third"})
}

func TestCompactionShrinksLogAndPreservesState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, _ := Open(path, 0, 10*time.Millisecond)
	q, _ := mgr.Create(Config{Name: "q", Mode: FIFO})

	// Churn: most of these are acked, so the log is mostly dead weight.
	for i := 0; i < 200; i++ {
		q.Enqueue(fmt.Sprintf("msg-%d", i), 0, 0)
	}
	for i := 0; i < 195; i++ {
		msgs, _ := q.Receive(context.Background(), 1, time.Minute, 0)
		q.Ack(msgs[0].Receipt)
	}

	before := mgr.Offset()
	if err := mgr.Compact(); err != nil {
		t.Fatal(err)
	}
	after := mgr.Offset()
	if after >= before {
		t.Fatalf("compaction did not shrink the log: %d -> %d", before, after)
	}
	mgr.Close()

	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("compacted log did not reopen: %v", err)
	}
	defer mgr2.Close()
	q2, _ := mgr2.Get("q")

	eq(t, drain(t, q2, 10), []string{"msg-195", "msg-196", "msg-197", "msg-198", "msg-199"})
}

func TestReplayFromLogOffset(t *testing.T) {
	mgr, q, _ := open(t, Config{Name: "q", Mode: FIFO})

	q.Enqueue("before-bookmark", 0, 0)
	bookmark := mgr.Offset()
	q.Enqueue("after-bookmark", 0, 0)

	// Consume everything so the queue is empty.
	msgs, _ := q.Receive(context.Background(), 10, time.Minute, 0)
	for _, m := range msgs {
		q.Ack(m.Receipt)
	}
	if s := q.Stats(); s.Ready != 0 {
		t.Fatalf("expected an empty queue, got %d ready", s.Ready)
	}

	// Replay only what happened after the bookmark.
	n, err := mgr.Replay("q", bookmark, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("replayed %d, want 1", n)
	}
	eq(t, drain(t, q, 10), []string{"after-bookmark"})
}

// --- concurrency ---

// The invariant that matters: with acks arriving well inside the visibility
// window, every message is delivered exactly once across all consumers. No
// message is lost and none is handed to two consumers at the same time.
//
// Run this with -race. It is the evidence behind the concurrency claim.
func TestConcurrentNoLossNoDuplicates(t *testing.T) {
	const (
		producers = 8
		perProd   = 250
		consumers = 8
		total     = producers * perProd
	)

	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProd; i++ {
				if _, err := q.Enqueue(fmt.Sprintf("p%d-%d", p, i), 0, 0); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	var mu sync.Mutex
	seen := make(map[string]int, total)
	done := make(chan struct{})

	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				msgs, err := q.Receive(context.Background(), 10, time.Minute, 20*time.Millisecond)
				if err != nil {
					t.Errorf("receive: %v", err)
					return
				}
				for _, m := range msgs {
					mu.Lock()
					seen[m.Body]++
					n := len(seen)
					mu.Unlock()
					if err := q.Ack(m.Receipt); err != nil {
						t.Errorf("ack: %v", err)
					}
					if n == total {
						close(done)
					}
				}
			}
		}()
	}

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out: consumers never drained the queue")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("lost messages: saw %d distinct, want %d", len(seen), total)
	}
	for body, count := range seen {
		if count != 1 {
			t.Fatalf("message %q delivered %d times, want exactly 1", body, count)
		}
	}
}

// Two consumers hammering the same queue must never both hold the same
// message. This is a tighter version of the test above aimed squarely at the
// ready-to-inflight transition.
func TestConcurrentLeasesAreExclusive(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	for i := 0; i < 500; i++ {
		q.Enqueue(fmt.Sprintf("m%d", i), 0, 0)
	}

	var mu sync.Mutex
	held := make(map[string]bool)
	var wg sync.WaitGroup

	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgs, err := q.Receive(context.Background(), 5, time.Minute, 0)
				if err != nil {
					t.Errorf("receive: %v", err)
					return
				}
				if len(msgs) == 0 {
					return
				}
				for _, m := range msgs {
					mu.Lock()
					if held[m.ID] {
						mu.Unlock()
						t.Errorf("message %s leased to two consumers at once", m.ID)
						return
					}
					held[m.ID] = true
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if len(held) != 500 {
		t.Fatalf("leased %d distinct messages, want 500", len(held))
	}
}
