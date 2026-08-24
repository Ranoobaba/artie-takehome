package queue

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrQueueExists   = errors.New("queue already exists")
	ErrQueueNotFound = errors.New("queue not found")

	// ErrStaleBookmark reports an offset from a previous epoch. Compaction
	// rewrites the log from byte zero, so an old offset does not name the
	// bytes the caller thinks it names.
	ErrStaleBookmark = errors.New("bookmark is from an earlier epoch and no longer describes the log")
)

// Bookmark is an opaque position in the log. Both halves matter: an offset is
// only meaningful within the epoch it was issued in.
type Bookmark struct {
	Epoch  uint64 `json:"epoch"`
	Offset int64  `json:"offset"`
}

// Manager owns every queue and the single shared log they all write to.
//
// One log rather than one per queue keeps recovery to a single sequential
// pass and gives every record a total order, which is what makes replay by
// offset meaningful across the whole system.
//
// Lock ordering is always Manager, then Queue, then WAL, never the reverse.
type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Queue
	wal    *WAL

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Open opens the log at path, rebuilds all state from it, and starts the
// background pump.
func Open(path string, syncEvery, tick time.Duration) (*Manager, error) {
	w, err := OpenWAL(path, syncEvery)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		queues: make(map[string]*Queue),
		wal:    w,
		stop:   make(chan struct{}),
	}
	if err := m.recover(); err != nil {
		w.Close()
		return nil, fmt.Errorf("recover: %w", err)
	}
	if tick <= 0 {
		tick = 50 * time.Millisecond
	}
	m.wg.Add(1)
	go m.pump(tick)
	return m, nil
}

// recover rebuilds every queue by replaying the log from the first byte.
//
// The rule that makes this simple: a lease never survives a restart. Any
// message that was checked out to a consumer when the process died goes back
// to ready, because that consumer is definitionally gone. Attempt counts and
// nack backoffs do survive, so a message that reliably kills its consumer
// still reaches the dead letter queue instead of looping forever.
func (m *Manager) recover() error {
	type staging struct {
		live   map[string]*Msg
		dlq    map[string]bool
		maxSeq uint64
	}
	pending := make(map[string]*staging)

	get := func(name string) *staging {
		s, ok := pending[name]
		if !ok {
			s = &staging{live: make(map[string]*Msg), dlq: make(map[string]bool)}
			pending[name] = s
		}
		return s
	}
	bump := func(s *staging, seq uint64) {
		if seq > s.maxSeq {
			s.maxSeq = seq
		}
	}

	err := m.wal.Recover(func(rec Record, _ int64) error {
		switch rec.Op {
		case OpCreateQueue:
			if rec.Config == nil {
				return nil
			}
			if _, ok := m.queues[rec.Queue]; !ok {
				m.queues[rec.Queue] = New(*rec.Config, m.wal)
			}
			// Compaction stamps the high water sequence number here, so a
			// queue that was fully drained before compacting does not restart
			// numbering at 1 and reuse values older records already used.
			bump(get(rec.Queue), rec.Seq)

		case OpEnqueue:
			if rec.Msg == nil {
				return nil
			}
			s := get(rec.Queue)
			cp := *rec.Msg
			s.live[cp.ID] = &cp
			bump(s, cp.Seq)

		case OpLease:
			// The lease itself is discarded, but the attempt count it
			// recorded is not: that is the only durable evidence of how many
			// times we have tried this message.
			if rec.Msg == nil {
				return nil
			}
			if cur, ok := get(rec.Queue).live[rec.Msg.ID]; ok {
				cur.Attempts = rec.Msg.Attempts
			}

		case OpNack:
			// A nack carries the consumer's backoff. Without applying it here
			// a restart makes every backed off message visible at once.
			if rec.Msg == nil {
				return nil
			}
			if cur, ok := get(rec.Queue).live[rec.Msg.ID]; ok {
				cur.Attempts = rec.Msg.Attempts
				cur.VisibleAt = rec.Msg.VisibleAt
			}

		case OpAck:
			s := get(rec.Queue)
			delete(s.live, rec.ID)
			delete(s.dlq, rec.ID)

		case OpDLQ:
			get(rec.Queue).dlq[rec.ID] = true

		case OpReplay:
			s := get(rec.Queue)
			delete(s.dlq, rec.ID)
			if cur, ok := s.live[rec.ID]; ok {
				cur.Attempts = 0
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	for name, s := range pending {
		q, ok := m.queues[name]
		if !ok {
			if len(s.live) > 0 || len(s.dlq) > 0 {
				// Messages for a queue whose creation record did not survive.
				// Dropping them silently is how a compaction race turns into
				// invisible data loss, so refuse to start instead.
				return fmt.Errorf("log holds %d messages for unknown queue %q", len(s.live), name)
			}
			continue
		}
		var live, dead []*Msg
		for id, msg := range s.live {
			if s.dlq[id] {
				dead = append(dead, msg)
			} else {
				live = append(live, msg)
			}
		}
		// Restore in sequence order so recovery is deterministic, which makes
		// it far easier to test and to reason about.
		sort.Slice(live, func(i, j int) bool { return live[i].Seq < live[j].Seq })
		sort.Slice(dead, func(i, j int) bool { return dead[i].Seq < dead[j].Seq })
		q.restore(live, dead, s.maxSeq)
	}
	return nil
}

// Create registers a new queue and makes it durable before returning.
func (m *Manager) Create(cfg Config) (*Queue, error) {
	if cfg.Name == "" {
		return nil, errors.New("queue name is required")
	}
	// The name is a path segment. A name containing a slash would be accepted
	// and durably logged, then 404 on every later request because the router
	// cannot match it, leaving a queue that can never be drained.
	if strings.ContainsAny(cfg.Name, "/?#% ") {
		return nil, errors.New(`queue name may not contain "/", "?", "#", "%" or spaces`)
	}
	if cfg.Mode != FIFO && cfg.Mode != LIFO {
		return nil, fmt.Errorf("mode must be %q or %q", FIFO, LIFO)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[cfg.Name]; ok {
		return nil, ErrQueueExists
	}
	q := New(cfg, m.wal)
	stored := q.Config()
	if err := m.wal.AppendSync(Record{Op: OpCreateQueue, Queue: cfg.Name, Config: &stored}); err != nil {
		return nil, err
	}
	m.queues[cfg.Name] = q
	return q, nil
}

func (m *Manager) Get(name string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

func (m *Manager) List() []Stats {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	out := make([]Stats, 0, len(qs))
	for _, q := range qs {
		out = append(out, q.Stats())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Bookmark is a position a client can record now and replay from later.
func (m *Manager) Bookmark() Bookmark {
	epoch, offset := m.wal.Bookmark()
	return Bookmark{Epoch: epoch, Offset: offset}
}

// Err reports a latched storage failure.
func (m *Manager) Err() error { return m.wal.Err() }

// Replay re-enqueues messages that were originally enqueued to a queue at or
// after a bookmark, optionally filtered to those enqueued after a wall clock
// time.
//
// The replayed messages are genuinely new: new IDs, new sequence numbers,
// zero attempts. Resurrecting the original IDs would collide with any copy
// still live in the queue and would corrupt attempt counts. Replay produces
// new deliveries of old content, which is semantics a consumer can reason
// about.
//
// The count is returned even on failure, because the messages published
// before the failure are already live and a caller who retries blindly would
// duplicate them.
func (m *Manager) Replay(name string, bm Bookmark, since time.Time) (int, error) {
	q, err := m.Get(name)
	if err != nil {
		return 0, err
	}

	epoch, _ := m.wal.Bookmark()
	if bm.Offset > 0 && bm.Epoch != epoch {
		// Without this the offset is reinterpreted against rewritten bytes:
		// either it silently finds nothing, or it re-enqueues the live
		// backlog that compaction rewrote as fresh looking history.
		return 0, fmt.Errorf("%w (bookmark epoch %d, log epoch %d)", ErrStaleBookmark, bm.Epoch, epoch)
	}

	type item struct {
		body     string
		priority int
	}
	var found []item

	// Collect first, enqueue second. Enqueuing inside the scan callback would
	// append to the log we are still reading, so a replay could feed itself
	// its own output and never terminate.
	err = m.wal.Scan(bm.Offset, func(rec Record, _ int64) error {
		if rec.Op != OpEnqueue || rec.Queue != name || rec.Msg == nil {
			return nil
		}
		if !since.IsZero() && rec.Msg.EnqueuedAt.Before(since) {
			return nil
		}
		found = append(found, item{body: rec.Msg.Body, priority: rec.Msg.Priority})
		return nil
	})
	if err != nil {
		return 0, err
	}

	published := 0
	for _, it := range found {
		if _, err := q.Enqueue(it.body, it.priority, 0); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

// Compact rewrites the log with only the records needed to rebuild current
// state.
//
// This is stop the world, and it has to be. The snapshot and the rename must
// be one atomic step: any enqueue or ack that lands between them is erased by
// the rename, and erased silently, because its producer was already told the
// write was durable. Holding the manager lock and every queue lock for the
// duration is the price of that guarantee. Compaction is a rare maintenance
// operation, so blocking traffic briefly is the right trade against losing
// acknowledged writes.
func (m *Manager) Compact() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	// A deterministic order costs nothing and keeps the acquisition safe if
	// anything later takes more than one queue lock.
	sort.Slice(qs, func(i, j int) bool { return qs[i].cfg.Name < qs[j].cfg.Name })

	for _, q := range qs {
		q.mu.Lock()
	}
	defer func() {
		for i := len(qs) - 1; i >= 0; i-- {
			qs[i].mu.Unlock()
		}
	}()

	var live []Record
	for _, q := range qs {
		live = append(live, q.liveRecordsLocked()...)
	}
	return m.wal.Compact(live)
}

// pump drives every queue's clocks from one goroutine. One ticker for the
// whole process rather than one per queue keeps the number of timers
// independent of the number of queues.
func (m *Manager) pump(every time.Duration) {
	defer m.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			now := time.Now()
			m.mu.RLock()
			for _, q := range m.queues {
				q.Tick(now)
			}
			m.mu.RUnlock()
		case <-m.stop:
			return
		}
	}
}

// Close stops the pump and flushes the log. Safe to call more than once.
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		close(m.stop)
		m.wg.Wait()
		err = m.wal.Close()
	})
	return err
}

// restore reinstalls recovered messages. Called only during startup, before
// the queue is reachable by any request.
//
// The lifetime counters are deliberately left at zero: they count what this
// process has done, not what the log contains. Seeding them with a depth
// would make them mean two different things depending on whether a restart
// had happened.
func (q *Queue) restore(live, dead []*Msg, maxSeq uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	q.seq = maxSeq
	for _, msg := range live {
		msg.Receipt = ""
		msg.LeaseExpiry = time.Time{}
		q.placeLocked(msg, now)
	}
	for _, msg := range dead {
		msg.Receipt = ""
		msg.LeaseExpiry = time.Time{}
		q.dlq = append(q.dlq, msg)
	}
}

// liveRecordsLocked produces the minimum set of records that would rebuild
// this queue's current state. The caller must already hold q.mu and must keep
// holding it until the compaction completes.
func (q *Queue) liveRecordsLocked() []Record {
	cfg := q.cfg
	out := []Record{{Op: OpCreateQueue, Queue: cfg.Name, Config: &cfg, Seq: q.seq}}

	emit := func(m *Msg) {
		cp := *m
		cp.Receipt = ""
		cp.LeaseExpiry = time.Time{}
		out = append(out, Record{Op: OpEnqueue, Queue: cfg.Name, Msg: &cp})
	}
	for _, m := range q.ready.items {
		emit(m)
	}
	for _, m := range q.delayed.items {
		emit(m)
	}
	for _, m := range q.inflight.items {
		emit(m)
	}
	for _, m := range q.dlq {
		emit(m)
		out = append(out, Record{Op: OpDLQ, Queue: cfg.Name, ID: m.ID})
	}
	return out
}

// compile time assertion that msgHeap satisfies the standard library's
// interface, so a drifting method signature fails at build time.
var _ heap.Interface = (*msgHeap)(nil)
