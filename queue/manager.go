package queue

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrQueueExists   = errors.New("queue already exists")
	ErrQueueNotFound = errors.New("queue not found")
)

// Manager owns every queue and the single shared log they all write to.
//
// One log rather than one per queue keeps recovery to a single sequential
// pass and gives every record a total order, which is what makes replay by
// offset meaningful across the whole system.
//
// Lock ordering is always Manager then Queue then WAL, never the reverse.
// Nothing acquires them in a different order, which is why there is no
// deadlock even though all three are held on some paths.
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
// to ready, because that consumer is definitionally gone. We keep the attempt
// count so that a message which crashes its consumer repeatedly still reaches
// the dead letter queue instead of looping forever.
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

	err := m.wal.Scan(0, func(rec Record, _ int64) error {
		switch rec.Op {
		case OpCreateQueue:
			if rec.Config == nil {
				return nil
			}
			if _, ok := m.queues[rec.Queue]; !ok {
				m.queues[rec.Queue] = New(*rec.Config, m.wal)
			}
			get(rec.Queue)

		case OpEnqueue:
			if rec.Msg == nil {
				return nil
			}
			s := get(rec.Queue)
			cp := *rec.Msg
			s.live[cp.ID] = &cp
			if cp.Seq > s.maxSeq {
				s.maxSeq = cp.Seq
			}

		case OpLease:
			// The lease itself is discarded, but the attempt count it
			// recorded is not: that is the only durable evidence of how many
			// times we have tried this message.
			if rec.Msg == nil {
				return nil
			}
			s := get(rec.Queue)
			if cur, ok := s.live[rec.Msg.ID]; ok {
				cur.Attempts = rec.Msg.Attempts
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
		// Restore in sequence order so the heaps are rebuilt from a
		// deterministic starting point. The heap does not require it, but a
		// deterministic recovery is far easier to test and to reason about.
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

// Offset is the current end of the log. A client can record it now and later
// ask to replay everything that happened after it, which is the bookmark half
// of the replay story.
func (m *Manager) Offset() int64 { return m.wal.Size() }

// Replay re-enqueues messages that were originally enqueued to a queue at or
// after a log offset, optionally filtered to those enqueued after a wall
// clock time.
//
// The replayed messages are genuinely new: new IDs, new sequence numbers,
// zero attempts. That is a deliberate choice. Resurrecting the original IDs
// would collide with any copy still live in the queue and would silently
// corrupt the attempt counts. Replay produces new deliveries of old content,
// which is the semantics a consumer can actually reason about.
func (m *Manager) Replay(name string, fromOffset int64, since time.Time) (int, error) {
	q, err := m.Get(name)
	if err != nil {
		return 0, err
	}

	type item struct {
		body     string
		priority int
	}
	var found []item

	// Collect first, enqueue second. Enqueuing inside the scan callback would
	// append to the log we are still reading, so a replay could feed itself
	// its own output and never terminate.
	err = m.wal.Scan(fromOffset, func(rec Record, _ int64) error {
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

	for _, it := range found {
		if _, err := q.Enqueue(it.body, it.priority, 0); err != nil {
			return 0, err
		}
	}
	return len(found), nil
}

// Compact rewrites the log with only the records needed to rebuild the
// current state of every queue.
func (m *Manager) Compact() error {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	var live []Record
	for _, q := range qs {
		live = append(live, q.liveRecords()...)
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

// Close stops the pump and flushes the log. It is safe to call more than
// once, which matters because the usual shutdown path has both a deferred
// close and an explicit one, and a double close should not be a crash.
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
	q.enqueued = uint64(len(live) + len(dead))
}

// liveRecords produces the minimum set of records that would rebuild this
// queue's current state, for compaction.
func (q *Queue) liveRecords() []Record {
	q.mu.Lock()
	defer q.mu.Unlock()

	cfg := q.cfg
	out := []Record{{Op: OpCreateQueue, Queue: cfg.Name, Config: &cfg}}

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
// interface. If a method signature drifts, this fails at build time rather
// than at the first push.
var _ heap.Interface = (*msgHeap)(nil)
