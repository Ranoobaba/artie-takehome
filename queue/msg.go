package queue

import "time"

// Mode selects how the ready heap breaks ties between messages that share a
// priority. It is one half of the ordering story; Priority is the other half.
// Delay is deliberately not a Mode, because it is not an ordering concern at
// all: it decides *when* a message becomes eligible, not *where* it sits in
// line once it is.
type Mode string

const (
	FIFO Mode = "fifo"
	LIFO Mode = "lifo"
)

// Msg is one unit of work moving through the system.
//
// A message is only ever in exactly one of the three heaps at a time
// (delayed, ready, or inflight), which is why a single `index` field is
// enough for heap bookkeeping.
type Msg struct {
	ID       string `json:"id"`
	Body     string `json:"body"`
	Priority int    `json:"priority"` // higher value wins, in both FIFO and LIFO

	// Seq is a monotonic arrival counter, assigned on enqueue. It is the
	// FIFO/LIFO sort key and it is what makes ordering total: two messages
	// can share a priority, but never a Seq.
	Seq uint64 `json:"seq"`

	// VisibleAt is the delay axis. A message is invisible to consumers until
	// now >= VisibleAt. For a message with no delay this is simply its
	// enqueue time, so the same field covers both cases with no branching.
	VisibleAt time.Time `json:"visible_at"`

	// EnqueuedAt never changes once set, unlike VisibleAt which moves every
	// time the message is retried. Replay by timestamp needs a fixed point.
	EnqueuedAt time.Time `json:"enqueued_at"`

	// Attempts counts how many times this message has been handed to a
	// consumer. It drives the dead letter cutoff.
	Attempts int `json:"attempts"`

	// Receipt and LeaseExpiry are set only while the message is leased to a
	// consumer. Receipt is a fresh token per delivery, so a consumer holding
	// a stale receipt from an expired lease cannot acknowledge work that has
	// since been handed to somebody else.
	Receipt     string    `json:"receipt,omitempty"`
	LeaseExpiry time.Time `json:"lease_expiry,omitzero"`

	// index is maintained by the heap implementation so that a specific
	// message can be removed in O(log n) instead of scanning. See heaps.go.
	index int
}

// Ready reports whether the delay gate has opened for this message.
func (m *Msg) Ready(now time.Time) bool {
	return !m.VisibleAt.After(now)
}
