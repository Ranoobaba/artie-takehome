package queue

import (
	"context"
	"errors"
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

	n, err := q.ReplayDLQ()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
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

	before := mgr.Bookmark().Offset
	if err := mgr.Compact(); err != nil {
		t.Fatal(err)
	}
	after := mgr.Bookmark().Offset
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
	bookmark := mgr.Bookmark()
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
	// sync.Once, because a duplicate delivery would otherwise close this
	// twice and panic. The test must report that bug, not crash on it.
	var doneOnce sync.Once

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
					if n >= total {
						doneOnce.Do(func() { close(done) })
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

// --- regressions ---
//
// Everything below reproduces a defect found in review. Each one failed
// before the corresponding fix.

// A replay offset comes from an API client. Before the fix, every failure
// path in the scanner repaired the log, so naming an offset that does not
// land on a record boundary deleted everything after it and still answered
// 200. The bookmark-then-replay flow in the demo walks straight into this.
func TestReplayWithBadOffsetLeavesLogIntact(t *testing.T) {
	mgr, q, path := open(t, Config{Name: "q", Mode: FIFO})
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue(fmt.Sprintf("m%d", i), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	bm := mgr.Bookmark()

	// Offset 9 is inside the first record's body, not on a boundary.
	if _, err := mgr.Replay("q", Bookmark{Epoch: bm.Epoch, Offset: 9}, time.Time{}); err == nil {
		t.Fatal("a mid record offset was accepted")
	}
	if got := mgr.Bookmark().Offset; got != bm.Offset {
		t.Fatalf("replay truncated the log: %d bytes -> %d", bm.Offset, got)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != bm.Offset {
		t.Fatalf("file on disk shrank to %d, want %d", st.Size(), bm.Offset)
	}

	// An offset past the end must also be refused rather than acted on.
	if _, err := mgr.Replay("q", Bookmark{Epoch: bm.Epoch, Offset: bm.Offset + 5000}, time.Time{}); err == nil {
		t.Fatal("an offset past the end of the log was accepted")
	}

	// Everything still recovers.
	mgr.Close()
	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("log did not survive a rejected replay: %v", err)
	}
	defer mgr2.Close()
	q2, err := mgr2.Get("q")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, drain(t, q2, 10), []string{"m0", "m1", "m2", "m3", "m4"})
}

// Compaction rewrites the log from byte zero, so an offset issued before it
// no longer names the bytes the client thinks it names. Before epochs, a
// stale bookmark either silently found nothing or re-enqueued the live
// backlog that compaction had rewritten as fresh looking history.
func TestReplayRejectsStaleBookmark(t *testing.T) {
	mgr, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue(fmt.Sprintf("m%d", i), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	stale := mgr.Bookmark()

	msgs, _ := q.Receive(context.Background(), 10, time.Minute, 0)
	for _, msg := range msgs {
		if err := q.Ack(msg.Receipt); err != nil {
			t.Fatal(err)
		}
	}
	if err := mgr.Compact(); err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.Replay("q", stale, time.Time{}); !errors.Is(err, ErrStaleBookmark) {
		t.Fatalf("want ErrStaleBookmark, got %v", err)
	}
	if s := q.Stats(); s.Ready != 0 {
		t.Fatalf("a rejected replay still changed the queue: ready=%d", s.Ready)
	}

	// A bookmark taken after the compaction is valid again.
	fresh := mgr.Bookmark()
	q.Enqueue("post-compaction", 0, 0)
	msgs, _ = q.Receive(context.Background(), 10, time.Minute, 0)
	for _, msg := range msgs {
		q.Ack(msg.Receipt)
	}
	n, err := mgr.Replay("q", fresh, time.Time{})
	if err != nil {
		t.Fatalf("a current bookmark was rejected: %v", err)
	}
	if n != 1 {
		t.Fatalf("replayed %d, want 1", n)
	}
}

// Compaction used to snapshot each queue under its own lock, release
// everything, and only then rewrite the log, so any write that fsynced and
// was acknowledged in that window was erased by the rename. Silently, because
// the producer had already been told it was durable.
func TestCompactDoesNotLoseAcknowledgedWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, err := Open(path, time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	q, err := mgr.Create(Config{Name: "q", Mode: FIFO})
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu       sync.Mutex
		accepted []string
		wg       sync.WaitGroup
	)
	stop := make(chan struct{})

	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				body := fmt.Sprintf("p%d-%d", p, i)
				if _, err := q.Enqueue(body, 0, 0); err == nil {
					// Only messages we were told succeeded are required to
					// come back. That is exactly the promise being tested.
					mu.Lock()
					accepted = append(accepted, body)
					mu.Unlock()
				}
			}
		}(p)
	}

	for i := 0; i < 3; i++ {
		time.Sleep(40 * time.Millisecond)
		if err := mgr.Compact(); err != nil {
			t.Errorf("compact: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}

	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	defer mgr2.Close()
	q2, err := mgr2.Get("q")
	if err != nil {
		t.Fatalf("queue did not survive compaction: %v", err)
	}

	got := make(map[string]bool)
	for {
		msgs, err := q2.Receive(context.Background(), 500, time.Minute, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			got[m.Body] = true
		}
	}

	mu.Lock()
	defer mu.Unlock()
	var missing []string
	for _, b := range accepted {
		if !got[b] {
			missing = append(missing, b)
		}
	}
	if len(missing) > 0 {
		show := missing
		if len(show) > 5 {
			show = show[:5]
		}
		t.Fatalf("%d of %d acknowledged writes lost across compaction, e.g. %v",
			len(missing), len(accepted), show)
	}
	if len(accepted) == 0 {
		t.Fatal("the test produced nothing, so it proved nothing")
	}
}

// A nack carries the consumer's backoff. The nack record used to hold only an
// ID and recovery had no case for it at all, so a restart rebuilt the message
// from its original enqueue record and made it visible immediately. During a
// rolling restart every backed off message in the system would land at once,
// straight back onto the downstream that was already struggling.
func TestNackBackoffSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	q, err := mgr.Create(Config{Name: "q", Mode: FIFO})
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue("work", 0, 0)

	msgs, _ := q.Receive(context.Background(), 1, time.Minute, 0)
	if err := q.Nack(msgs[0].Receipt, time.Hour); err != nil {
		t.Fatal(err)
	}
	if s := q.Stats(); s.Delayed != 1 || s.Ready != 0 {
		t.Fatalf("before restart: ready=%d delayed=%d, want 0 and 1", s.Ready, s.Delayed)
	}
	mgr.Close()

	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()
	q2, err := mgr2.Get("q")
	if err != nil {
		t.Fatal(err)
	}

	if s := q2.Stats(); s.Delayed != 1 || s.Ready != 0 {
		t.Fatalf("backoff erased by restart: ready=%d delayed=%d, want 0 and 1", s.Ready, s.Delayed)
	}
	if got := drain(t, q2, 1); len(got) != 0 {
		t.Fatalf("a message with an hour of backoff left was delivered immediately: %v", got)
	}
	if got := q2.Stats().Delivered; got != 0 {
		t.Fatalf("delivered=%d after restart, want 0", got)
	}
}

// Compaction of a fully drained queue used to leave nothing carrying the
// sequence number, so recovery restarted numbering at 1 and handed out values
// that older records had already used.
func TestSequenceSurvivesCompactionOfDrainedQueue(t *testing.T) {
	mgr, q, path := open(t, Config{Name: "q", Mode: FIFO})
	for i := 0; i < 5; i++ {
		q.Enqueue(fmt.Sprintf("m%d", i), 0, 0)
	}
	msgs, _ := q.Receive(context.Background(), 10, time.Minute, 0)
	for _, m := range msgs {
		if err := q.Ack(m.Receipt); err != nil {
			t.Fatal(err)
		}
	}
	if err := mgr.Compact(); err != nil {
		t.Fatal(err)
	}
	mgr.Close()

	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()
	q2, _ := mgr2.Get("q")

	m, err := q2.Enqueue("after", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq <= 5 {
		t.Fatalf("sequence restarted at %d, reusing a number older records already used", m.Seq)
	}
}

// A name containing a path separator was accepted and durably logged, then
// 404ed on every later request because the router could not match it, leaving
// a queue that could never be drained or removed.
func TestQueueNameRejectsPathSeparators(t *testing.T) {
	mgr, _, _ := open(t, Config{Name: "ok", Mode: FIFO})
	for _, bad := range []string{"tenant/orders", "a?b", "a#b", "a b"} {
		if _, err := mgr.Create(Config{Name: bad, Mode: FIFO}); err == nil {
			t.Errorf("accepted %q, which the router can never match", bad)
		}
	}
}

// Scan used to read the w.f field with no lock while Compact reassigned it and
// closed the old handle, and the resulting read error routed into the repair
// path and amputated the freshly compacted log. Run under -race.
func TestConcurrentReplayAndCompact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, err := Open(path, time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	q, err := mgr.Create(Config{Name: "q", Mode: FIFO})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if _, err := q.Enqueue(fmt.Sprintf("m%d", i), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	bm := mgr.Bookmark()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Either outcome is acceptable. Destroying the log is not.
		mgr.Replay("q", bm, time.Time{})
	}()
	go func() {
		defer wg.Done()
		mgr.Compact()
	}()
	wg.Wait()
	mgr.Close()

	mgr2, err := Open(path, 0, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("log unusable after a concurrent replay and compaction: %v", err)
	}
	defer mgr2.Close()
	q2, err := mgr2.Get("q")
	if err != nil {
		t.Fatalf("queue lost: %v", err)
	}
	if s := q2.Stats(); s.Ready < 300 {
		t.Fatalf("lost messages: ready=%d, want at least 300", s.Ready)
	}
}

// --- lease extension ---

// A job that runs longer than the visibility timeout must be able to hold its
// claim. Without this the queue can only serve jobs shorter than the timeout.
func TestExtendKeepsLeaseAlive(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("long-job", 0, 0)

	msgs, _ := q.Receive(context.Background(), 1, 80*time.Millisecond, 0)
	if len(msgs) != 1 {
		t.Fatal("no message")
	}
	receipt := msgs[0].Receipt

	// Heartbeat three times, each before the current lease runs out.
	for i := 0; i < 3; i++ {
		time.Sleep(50 * time.Millisecond)
		if _, err := q.Extend(receipt, 80*time.Millisecond); err != nil {
			t.Fatalf("heartbeat %d: %v", i+1, err)
		}
	}

	// Well past the original 80ms expiry, and it is still ours.
	if got := drain(t, q, 1); len(got) != 0 {
		t.Fatalf("message was redelivered despite heartbeats: %v", got)
	}
	if err := q.Ack(receipt); err != nil {
		t.Fatalf("the original receipt should still be valid: %v", err)
	}
}

// Extending is not a way to reclaim a lease that already lapsed. By then the
// message may belong to another consumer.
func TestExtendRejectsLapsedLease(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("work", 0, 0)

	msgs, _ := q.Receive(context.Background(), 1, 50*time.Millisecond, 0)
	stale := msgs[0].Receipt

	time.Sleep(120 * time.Millisecond)
	q.Tick(time.Now())

	if _, err := q.Extend(stale, time.Minute); err != ErrNoLease {
		t.Fatalf("a lapsed lease was extended, got %v", err)
	}
}

// The inflight heap is ordered by lease expiry and the pump only ever looks at
// its root, so changing an expiry without re-heapifying hides every lease
// behind it. This is the test for the heap.Fix call in Extend.
func TestExtendReordersInflightHeap(t *testing.T) {
	_, q, _ := open(t, Config{Name: "q", Mode: FIFO})
	q.Enqueue("a", 0, 0)
	q.Enqueue("b", 0, 0)

	first, _ := q.Receive(context.Background(), 1, 60*time.Millisecond, 0)
	second, _ := q.Receive(context.Background(), 1, 200*time.Millisecond, 0)
	if len(first) != 1 || len(second) != 1 || first[0].Body != "a" || second[0].Body != "b" {
		t.Fatalf("setup: got %v and %v", first, second)
	}

	// "a" sits at the root because it expires soonest. Pushing it past "b"
	// must move it, or the pump peeks at "a", sees a future expiry, stops,
	// and never reclaims "b" at all.
	if _, err := q.Extend(first[0].Receipt, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	q.Tick(time.Now())

	if s := q.Stats(); s.Ready != 1 || s.Inflight != 1 {
		t.Fatalf("ready=%d inflight=%d, want 1 and 1: the extended lease hid the expired one", s.Ready, s.Inflight)
	}
	if got := drain(t, q, 5); len(got) != 1 || got[0] != "b" {
		t.Fatalf("got %v, want [b]", got)
	}
}
