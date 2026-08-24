package queue

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

var (
	// ErrNoLease is returned when a receipt does not match any outstanding
	// lease. The usual cause is that the lease already expired and the
	// message was handed to a different consumer, so the caller's work is no
	// longer authoritative.
	ErrNoLease = errors.New("unknown or expired receipt")

	// ErrPriorityDisabled keeps the queue's contract explicit: a queue
	// created without priority will not silently accept and then ignore a
	// priority, because a producer that thinks it is setting priority and is
	// not is a very quiet bug.
	ErrPriorityDisabled = errors.New("queue was not created with priority enabled")
)

// Config is the shape of a queue, chosen at creation time and then fixed.
//
// Note what is NOT here: there is no "delay mode". Delay is a property of an
// individual message, not of the queue, which is why a delayed priority LIFO
// needs no configuration beyond Mode and Priority.
type Config struct {
	Name     string `json:"name"`
	Mode     Mode   `json:"mode"`
	Priority bool   `json:"priority"`

	// MaxAttempts is the delivery count after which a message is parked in
	// the dead letter queue instead of being redelivered forever. Zero means
	// retry without limit.
	MaxAttempts int `json:"max_attempts"`

	// VisibilityTimeoutMS is the default lease duration in milliseconds, used
	// when a consumer does not name one on receive. It is an int rather than
	// a time.Duration so that one struct can serve as the HTTP request body,
	// the in memory config, and the log record with no translation layer
	// between them. A time.Duration would serialise as raw nanoseconds, which
	// is unreadable in the log and hostile in an API.
	VisibilityTimeoutMS int `json:"visibility_timeout_ms"`
}

// Visibility is the configured lease duration.
func (c Config) Visibility() time.Duration {
	return time.Duration(c.VisibilityTimeoutMS) * time.Millisecond
}

// Stats is a point in time snapshot, safe to hand to a caller because every
// field is a value.
type Stats struct {
	Name      string `json:"name"`
	Mode      Mode   `json:"mode"`
	Priority  bool   `json:"priority"`
	Ready     int    `json:"ready"`
	Delayed   int    `json:"delayed"`
	Inflight  int    `json:"inflight"`
	DLQ       int    `json:"dlq"`
	Enqueued  uint64 `json:"total_enqueued"`
	Delivered uint64 `json:"total_delivered"`
	Acked     uint64 `json:"total_acked"`
	Nacked    uint64 `json:"total_nacked"`
	Expired   uint64 `json:"total_lease_expiries"`

	// OldestReadyAgeMS is the first number an operator asks for when a queue
	// looks wedged. It measures from EnqueuedAt, which never moves, rather
	// than from VisibleAt, which is rewritten on every redelivery and would
	// reset the metric exactly when a message is having trouble.
	OldestReadyAgeMS int64 `json:"oldest_ready_age_ms"`

	// Degraded reports a storage error that happened on a background path,
	// where there was no request left to return it to.
	Degraded string `json:"degraded,omitempty"`
}

// Queue is a single named queue: three heaps, a lock, and a write ahead log.
//
// The three heaps hold disjoint sets of messages and together they hold every
// live message:
//
//	delayed  messages whose VisibleAt is still in the future
//	ready    messages eligible for delivery, ordered by the queue's comparator
//	inflight messages leased to a consumer and awaiting ack
//
// Movement between them is driven by two clocks (VisibleAt and LeaseExpiry),
// both of which are the root of a min heap, so checking whether there is any
// work to do is O(1) and doing it is O(log n).
type Queue struct {
	cfg Config
	wal *WAL

	mu       sync.Mutex
	seq      uint64
	ready    *msgHeap
	delayed  *msgHeap
	inflight *msgHeap
	leases   map[string]*Msg // receipt -> message, for O(1) ack
	dlq      []*Msg

	enqueued, delivered, acked, nacked, expired uint64

	// walErr latches a storage failure that happened on the background pump,
	// where no caller was waiting to be told. It surfaces through Stats.
	walErr error

	// wake is closed to broadcast to every waiting receiver and then
	// replaced. A buffered channel would wake exactly one waiter, which
	// leaves the rest sitting out their full timeout while work is ready.
	wake chan struct{}
}

// New builds an empty queue. Recovery from the log is handled separately by
// the Manager, which replays records through recover.
func New(cfg Config, w *WAL) *Queue {
	if cfg.Mode == "" {
		cfg.Mode = FIFO
	}
	if cfg.VisibilityTimeoutMS <= 0 {
		cfg.VisibilityTimeoutMS = 30000
	}
	return &Queue{
		cfg:      cfg,
		wal:      w,
		ready:    newMsgHeap(orderBy(cfg.Mode)),
		delayed:  newMsgHeap(byVisibleAt),
		inflight: newMsgHeap(byLeaseExpiry),
		leases:   make(map[string]*Msg),
		wake:     make(chan struct{}),
	}
}

func (q *Queue) Config() Config { return q.cfg }

// Enqueue durably records a new message and then makes it visible.
//
// Ordering matters here and is the core of the durability guarantee: the log
// record is appended and fsynced BEFORE the message enters any heap and
// before this function returns. A crash between the fsync and the heap insert
// is safe, because replay will find the record and rebuild the state. The
// reverse order would not be safe: we would have acknowledged a message to
// the producer that exists only in memory.
func (q *Queue) Enqueue(body string, priority int, delay time.Duration) (Msg, error) {
	if priority != 0 && !q.cfg.Priority {
		return Msg{}, ErrPriorityDisabled
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	q.seq++
	m := &Msg{
		ID:         newID(),
		Body:       body,
		Priority:   priority,
		Seq:        q.seq,
		VisibleAt:  now.Add(delay),
		EnqueuedAt: now,
	}

	// Durable first, visible second.
	//
	// The sequence number is deliberately NOT rolled back on failure. A write
	// can fail after its bytes are already framed at their final offset, so
	// reusing the number would let two live messages share a Seq. That breaks
	// the total ordering the comparator depends on, and it is far worse than
	// a gap in the sequence, which nothing depends on.
	if err := q.wal.AppendSync(Record{Op: OpEnqueue, Queue: q.cfg.Name, Msg: m}); err != nil {
		return Msg{}, err
	}

	q.placeLocked(m, now)
	q.enqueued++
	q.signalLocked()
	return *m, nil
}

// Receive leases up to max messages to a consumer.
//
// This is a lease, not a pop. The message is moved to the inflight heap and
// stays there until it is acked or its lease expires, at which point it
// returns to ready with an incremented attempt count. That is what makes a
// consumer crash safe: a destructive pop would lose the message the moment
// the consumer died holding it.
//
// wait > 0 turns this into a long poll, so an idle consumer parks on a
// channel instead of hammering the endpoint in a spin loop. ctx cancels the
// wait, so a consumer that hangs up frees its server goroutine immediately.
func (q *Queue) Receive(ctx context.Context, max int, visibility, wait time.Duration) ([]Msg, error) {
	if max <= 0 {
		max = 1
	}
	if visibility <= 0 {
		visibility = q.cfg.Visibility()
	}
	deadline := time.Now().Add(wait)

	for {
		out, wake, err := q.tryReceive(max, visibility)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 || wait <= 0 {
			return out, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		// Cap the sleep so a message whose delay elapses still wakes us
		// promptly even if nothing signals.
		if remaining > 50*time.Millisecond {
			remaining = 50 * time.Millisecond
		}
		timer := time.NewTimer(remaining)
		select {
		case <-wake:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
		timer.Stop()
	}
}

// tryReceive makes one non blocking attempt. It also returns the wake channel
// it observed, captured under the lock, so a caller that finds nothing cannot
// miss a signal that arrives before it starts waiting.
func (q *Queue) tryReceive(max int, visibility time.Duration) ([]Msg, chan struct{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	wake := q.wake
	now := time.Now()
	q.advanceLocked(now)

	var picked []*Msg
	for len(picked) < max {
		m := q.ready.peek()
		if m == nil {
			break
		}
		heap.Pop(q.ready)
		picked = append(picked, m)
	}
	if len(picked) == 0 {
		return nil, wake, nil
	}

	// Work out the post lease state and log it BEFORE touching the messages.
	//
	// Enqueue and Ack both write the log first, and this path has to as well.
	// Mutating first and then failing would leave the messages leased under
	// receipts nobody ever received, invisible for the whole visibility
	// window, with an attempt burned against a delivery that never happened.
	type lease struct {
		receipt string
		expiry  time.Time
	}
	leases := make([]lease, len(picked))
	for i, m := range picked {
		leases[i] = lease{receipt: newID(), expiry: now.Add(visibility)}

		// The record carries what the message is about to become, not what it
		// is now, so recovery sees the attempt that is being handed out.
		snap := *m
		snap.Attempts++
		snap.Receipt = leases[i].receipt
		snap.LeaseExpiry = leases[i].expiry

		// Lease records are appended but deliberately NOT fsynced. Losing one
		// in a crash only causes a redelivery, which at least once delivery
		// already permits. Paying an fsync to prevent a duplicate we are
		// allowed to produce anyway would be the wrong trade.
		if err := q.wal.Append(Record{Op: OpLease, Queue: q.cfg.Name, Msg: &snap}); err != nil {
			// Put everything back exactly as it was. The heap restores its
			// own invariant on push, so order is unaffected.
			for _, back := range picked {
				heap.Push(q.ready, back)
			}
			return nil, wake, err
		}
	}

	// Commit.
	out := make([]Msg, 0, len(picked))
	for i, m := range picked {
		m.Attempts++
		m.Receipt = leases[i].receipt
		m.LeaseExpiry = leases[i].expiry
		heap.Push(q.inflight, m)
		q.leases[m.Receipt] = m
		q.delivered++

		// Return copies. The queue keeps mutating these messages after the
		// lock is released, so handing out the live pointers would be a data
		// race, and one the race detector does catch.
		out = append(out, *m)
	}
	return out, wake, nil
}

// Ack confirms the work is done and permanently removes the message.
func (q *Queue) Ack(receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.leases[receipt]
	if !ok {
		return ErrNoLease
	}
	if err := q.wal.AppendSync(Record{Op: OpAck, Queue: q.cfg.Name, ID: m.ID}); err != nil {
		return err
	}
	heap.Remove(q.inflight, m.index) // O(log n), no scan, thanks to the cached index
	delete(q.leases, receipt)
	q.acked++
	return nil
}

// Extend renews a lease that is still live, so a consumer whose work runs
// longer than the visibility timeout keeps its claim instead of having the
// message redelivered underneath it while it is still being worked on.
//
// Without this the queue can only serve jobs shorter than the visibility
// timeout, because the alternative is setting a timeout long enough for the
// slowest possible job, which then delays recovery from every crash by the
// same amount.
//
// Nothing is written to the log. A lease is not durable in the first place,
// since recovery voids every lease and returns its message to ready, so an
// extension has nothing to persist. That makes a heartbeat free on disk,
// which matters because a consumer may send one every few seconds.
func (q *Queue) Extend(receipt string, visibility time.Duration) (time.Time, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.leases[receipt]
	if !ok {
		return time.Time{}, ErrNoLease
	}
	if visibility <= 0 {
		visibility = q.cfg.Visibility()
	}
	m.LeaseExpiry = time.Now().Add(visibility)

	// The inflight heap is ordered by LeaseExpiry and we just changed it, so
	// the heap invariant has to be restored. heap.Fix is O(log n) and works
	// from the cached index, so this costs no more than a push.
	heap.Fix(q.inflight, m.index)
	return m.LeaseExpiry, nil
}

// Nack returns a message to the queue without waiting for its lease to run
// out. delay lets a consumer apply its own backoff rather than being
// redelivered instantly into the same failure.
func (q *Queue) Nack(receipt string, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.leases[receipt]
	if !ok {
		return ErrNoLease
	}

	now := time.Now()
	heap.Remove(q.inflight, m.index)
	delete(q.leases, receipt)
	m.Receipt = ""
	m.LeaseExpiry = time.Time{}
	q.nacked++

	if err := q.retryLocked(m, now, delay); err != nil {
		return err
	}
	q.signalLocked()
	return nil
}

// Tick is the periodic pump: it opens delay gates and reclaims expired
// leases. It is called by the Manager's background goroutine, and also
// implicitly on every receive so behaviour does not depend on the ticker's
// phase.
func (q *Queue) Tick(now time.Time) {
	q.mu.Lock()
	q.advanceLocked(now)
	q.mu.Unlock()
}

// advanceLocked moves messages between heaps based on the two clocks. It must
// be called with q.mu held.
func (q *Queue) advanceLocked(now time.Time) {
	woke := false

	// Delay gate: everything whose VisibleAt has arrived becomes eligible.
	// Because delayed is a min heap on VisibleAt, we stop at the first
	// message that is not due yet rather than scanning the whole set.
	for {
		m := q.delayed.peek()
		if m == nil || !m.Ready(now) {
			break
		}
		heap.Pop(q.delayed)
		heap.Push(q.ready, m)
		woke = true
	}

	// Lease reclamation: a consumer that died holding a message stops holding
	// it here.
	for {
		m := q.inflight.peek()
		if m == nil || m.LeaseExpiry.After(now) {
			break
		}
		heap.Pop(q.inflight)
		delete(q.leases, m.Receipt)
		m.Receipt = ""
		m.LeaseExpiry = time.Time{}
		q.expired++
		if err := q.retryLocked(m, now, 0); err != nil && q.walErr == nil {
			// Nobody is waiting on this path, so the error is latched and
			// reported through Stats rather than dropped.
			q.walErr = err
		}
		woke = true
	}

	if woke {
		q.signalLocked()
	}
}

// retryLocked puts a failed message back, or parks it in the dead letter
// queue once it has burned through its attempts. Without this cutoff a single
// poison message would be redelivered forever and would keep a consumer
// permanently busy failing.
func (q *Queue) retryLocked(m *Msg, now time.Time, delay time.Duration) error {
	if q.cfg.MaxAttempts > 0 && m.Attempts >= q.cfg.MaxAttempts {
		// Dead lettering is durable. If this record is lost the cutoff is
		// silently undone on the next restart and the poison message loops
		// forever, which is the exact failure this function exists to stop.
		if err := q.wal.AppendSync(Record{Op: OpDLQ, Queue: q.cfg.Name, ID: m.ID}); err != nil {
			// Keep it inflight-free but visible rather than losing it.
			m.VisibleAt = now
			q.placeLocked(m, now)
			return err
		}
		q.dlq = append(q.dlq, m)
		return nil
	}

	m.VisibleAt = now.Add(delay)

	// The backoff has to be durable or a restart erases it. Recovery rebuilds
	// from the original enqueue record, whose VisibleAt is long past, so
	// without this every backed off message in the system becomes visible at
	// once the moment the process restarts.
	snap := *m
	if err := q.wal.AppendSync(Record{Op: OpNack, Queue: q.cfg.Name, ID: m.ID, Msg: &snap}); err != nil {
		return err
	}
	q.placeLocked(m, now)
	return nil
}

// placeLocked files a message into whichever heap its delay gate implies.
// This is the only place that decides between delayed and ready, so the two
// paths cannot drift apart.
func (q *Queue) placeLocked(m *Msg, now time.Time) {
	if m.Ready(now) {
		heap.Push(q.ready, m)
	} else {
		heap.Push(q.delayed, m)
	}
}

// ReplayDLQ moves parked messages back to the head of the pipeline with their
// attempt counts reset. This is the operational half of the replay story; the
// historical half lives in the WAL and is exposed by the Manager.
//
// It returns the number actually replayed even when it fails partway, so a
// caller that retries knows what already happened.
func (q *Queue) ReplayDLQ() (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	done := 0
	for i, m := range q.dlq {
		if err := q.wal.AppendSync(Record{Op: OpReplay, Queue: q.cfg.Name, ID: m.ID}); err != nil {
			q.dlq = q.dlq[i:]
			if done > 0 {
				q.signalLocked()
			}
			return done, err
		}
		m.Attempts = 0
		m.VisibleAt = now
		q.placeLocked(m, now)
		done++
	}
	q.dlq = nil
	if done > 0 {
		q.signalLocked()
	}
	return done, nil
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	s := Stats{
		Name:      q.cfg.Name,
		Mode:      q.cfg.Mode,
		Priority:  q.cfg.Priority,
		Ready:     q.ready.Len(),
		Delayed:   q.delayed.Len(),
		Inflight:  q.inflight.Len(),
		DLQ:       len(q.dlq),
		Enqueued:  q.enqueued,
		Delivered: q.delivered,
		Acked:     q.acked,
		Nacked:    q.nacked,
		Expired:   q.expired,
	}
	if q.walErr != nil {
		s.Degraded = q.walErr.Error()
	}
	// The heap root is the next message out, not necessarily the oldest, so
	// this is scanned. Ready depth is bounded in practice and this endpoint
	// is not on the hot path.
	oldest := time.Time{}
	for _, m := range q.ready.items {
		if oldest.IsZero() || m.EnqueuedAt.Before(oldest) {
			oldest = m.EnqueuedAt
		}
	}
	if !oldest.IsZero() {
		s.OldestReadyAgeMS = time.Since(oldest).Milliseconds()
	}
	return s
}

// signalLocked wakes every waiting receiver by closing the current wake
// channel and installing a fresh one. Must be called with q.mu held.
func (q *Queue) signalLocked() {
	close(q.wake)
	q.wake = make(chan struct{})
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
