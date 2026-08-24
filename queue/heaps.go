package queue

// msgHeap is a binary heap over *Msg whose ordering is supplied at
// construction time.
//
// There is one heap type, not three. The ready, delayed, and inflight heaps
// are all instances of this, differing only in their comparator. That is the
// same idea as the ordering modes themselves: rather than special casing
// FIFO, LIFO, and priority into separate structures, we express all of them
// as one comparison function over one structure. It is what lets
// "delayed priority LIFO" exist without a single extra branch.
//
// This implements heap.Interface from the standard library, so pushes and
// pops go through container/heap rather than being called directly.
type msgHeap struct {
	items []*Msg
	less  func(a, b *Msg) bool
}

func newMsgHeap(less func(a, b *Msg) bool) *msgHeap {
	return &msgHeap{less: less}
}

func (h *msgHeap) Len() int { return len(h.items) }

func (h *msgHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }

// Swap keeps each message's cached index in sync with its slot. That cached
// index is what makes removal of an arbitrary message O(log n): on ack we
// already hold a pointer to the message, so we never have to search for it.
func (h *msgHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *msgHeap) Push(x any) {
	m := x.(*Msg)
	m.index = len(h.items)
	h.items = append(h.items, m)
}

func (h *msgHeap) Pop() any {
	old := h.items
	n := len(old)
	m := old[n-1]
	old[n-1] = nil // release the reference so the message can be collected
	m.index = -1
	h.items = old[:n-1]
	return m
}

// peek returns the root of the heap without removing it, or nil when empty.
// Both time driven heaps rely on this: the promoter only needs to look at the
// single earliest deadline to decide whether there is any work to do.
func (h *msgHeap) peek() *Msg {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// orderBy builds the comparator for a queue's ready heap.
//
// This function is the entire "frankenstein" idea in six lines. Priority and
// arrival order are treated as one composite sort key rather than as two
// competing features, so every supported combination is the same code path:
//
//	priority FIFO   -> priority DESC, seq ASC
//	priority LIFO   -> priority DESC, seq DESC
//	plain FIFO      -> all priorities equal, so seq ASC
//	plain LIFO      -> all priorities equal, so seq DESC
//
// Priority always outranks arrival order. A queue that ignores priority
// simply has every message at the default priority, so the first comparison
// never fires and the mode alone decides.
func orderBy(mode Mode) func(a, b *Msg) bool {
	return func(a, b *Msg) bool {
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if mode == LIFO {
			return a.Seq > b.Seq
		}
		return a.Seq < b.Seq
	}
}

// byVisibleAt orders the delayed heap: soonest to become visible sits at the
// root, so the promoter can check one element to know if anything is due.
func byVisibleAt(a, b *Msg) bool { return a.VisibleAt.Before(b.VisibleAt) }

// byLeaseExpiry orders the inflight heap: soonest to time out sits at the
// root. Without this we would have to scan every outstanding lease on each
// tick to find the expired ones.
func byLeaseExpiry(a, b *Msg) bool { return a.LeaseExpiry.Before(b.LeaseExpiry) }
