package queue

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Op is the kind of mutation a log record describes.
type Op string

const (
	OpCreateQueue Op = "create_queue"
	OpEnqueue     Op = "enqueue"
	OpLease       Op = "lease"
	OpAck         Op = "ack"
	OpNack        Op = "nack"
	OpDLQ         Op = "dlq"
	OpReplay      Op = "replay"

	// OpEpoch is written as the first record of every compacted log. It is
	// how a bookmark handed out before a compaction is recognised as stale
	// rather than silently reinterpreted against rewritten bytes.
	OpEpoch Op = "epoch"
)

// Record is one entry in the write ahead log.
type Record struct {
	Op     Op      `json:"op"`
	Queue  string  `json:"q,omitempty"`
	ID     string  `json:"id,omitempty"`
	Msg    *Msg    `json:"m,omitempty"`
	Config *Config `json:"cfg,omitempty"`

	// Epoch is set only on OpEpoch records.
	Epoch uint64 `json:"epoch,omitempty"`

	// Seq carries a queue.s high water sequence number on the OpCreateQueue
	// record written by compaction, so a drained queue does not restart its
	// sequence at 1 and reuse numbers that older records already used.
	Seq uint64 `json:"seq,omitempty"`

	// At is the wall clock time the record was written, in unix nanoseconds.
	At int64 `json:"at"`
}

const (
	// recHeader is 4 bytes of body length followed by 4 bytes of CRC32.
	recHeader = 8

	// maxRecord bounds how much we will trust a length prefix, so a corrupt
	// length cannot make recovery allocate an arbitrary amount of memory.
	maxRecord = 32 << 20
)

// ErrCorrupt reports a record that failed its checksum or ran off the end of
// the log. Recovery repairs this; every other caller is told about it and
// changes nothing.
var ErrCorrupt = errors.New("corrupt log record")

// WAL is an append only write ahead log, and it is the entire storage engine.
//
// Storage could not be delegated to a separate queue or database, so this is
// built from the standard library alone: a file, a length prefix, and a
// checksum. Durability is append plus fsync before acknowledging. Recovery is
// a replay from byte zero.
//
// Two rules keep it safe under concurrency:
//
//   - Writes go through WriteAt at a tracked offset rather than through the
//     file's own cursor, so readers can never disturb a writer.
//   - Only recovery may modify the log during a read. Every other reader is
//     strictly read only, because a reader that repairs is a reader that can
//     delete data on behalf of whoever supplied its arguments.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	size int64

	// epoch increments on every compaction. Offsets are only comparable
	// within one epoch, because compaction rewrites the file from byte zero.
	epoch uint64

	// retired holds file handles replaced by compaction. They are kept open
	// until Close so that a scan already reading through one continues to see
	// a consistent snapshot instead of failing mid read. The unlinked inode
	// stays alive until its last descriptor closes, which is exactly the
	// behaviour we want here.
	retired []*os.File

	// failed latches the first unrecoverable storage error. Once set, every
	// subsequent write is refused rather than acknowledged, because the one
	// thing worse than returning an error is returning 201 for a message that
	// is not going to be there.
	failed error

	// syncEvery == 0 means fsync inline on every durability critical append,
	// which is the safe default. A positive value enables group commit.
	syncEvery time.Duration
	dirty     bool
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// OpenWAL opens or creates the log at path.
func OpenWAL(path string, syncEvery time.Duration) (*WAL, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &WAL{
		f:         f,
		path:      path,
		size:      st.Size(),
		syncEvery: syncEvery,
		stop:      make(chan struct{}),
	}
	if syncEvery > 0 {
		w.wg.Add(1)
		go w.flusher()
	}
	return w, nil
}

// Append writes a record without forcing it to disk. Used for transitions
// whose loss is already permitted by at least once delivery, such as taking
// out a lease: replaying without that record simply redelivers the message.
func (w *WAL) Append(rec Record) error { return w.write(rec, false) }

// AppendSync writes a record and does not return until it is durable.
func (w *WAL) AppendSync(rec Record) error { return w.write(rec, true) }

func (w *WAL) write(rec Record, durable bool) error {
	if rec.At == 0 {
		rec.At = time.Now().UnixNano()
	}
	buf, err := encode(rec)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.failed != nil {
		return fmt.Errorf("log is unusable: %w", w.failed)
	}
	if _, err := w.f.WriteAt(buf, w.size); err != nil {
		return err
	}
	w.size += int64(len(buf))

	if !durable {
		return nil
	}
	if w.syncEvery > 0 {
		w.dirty = true
		return nil
	}
	if err := w.f.Sync(); err != nil {
		// A failed fsync is not retryable on Linux: the kernel drops the
		// dirty pages. Latch it so no later write is acknowledged.
		w.failed = err
		return err
	}
	return nil
}

// Recover replays the whole log and is the ONLY caller permitted to repair a
// torn tail. A process killed mid write leaves a partial record; refusing to
// start would turn a routine crash into an outage, and trusting the bytes
// would corrupt state, so recovery truncates at the last good record.
func (w *WAL) Recover(fn func(Record, int64) error) error {
	return w.scan(0, true, fn)
}

// Scan reads records from a byte offset and is strictly read only.
//
// It cannot truncate. That distinction is the whole point of splitting it
// from Recover: Scan's offset comes from an API client, and a reader that
// repairs on error would let any caller delete the log by naming an offset
// that does not land on a record boundary.
func (w *WAL) Scan(from int64, fn func(Record, int64) error) error {
	return w.scan(from, false, fn)
}

func (w *WAL) scan(from int64, repair bool, fn func(Record, int64) error) error {
	if from < 0 {
		return fmt.Errorf("offset %d is negative", from)
	}

	// Capture the handle and the end of the log together under the lock. The
	// handle matters: compaction replaces w.f, and reading the field without
	// synchronisation is a data race whose failure mode is reading the new
	// file at offsets that describe the old one.
	w.mu.Lock()
	f, end := w.f, w.size
	w.mu.Unlock()

	if from > end {
		return fmt.Errorf("offset %d is past the end of the log (%d bytes)", from, end)
	}
	if from == end {
		return nil
	}

	// The callback is invoked with no lock held, because fn is allowed to
	// write back into the queue and would otherwise re-enter and deadlock.
	r := bufio.NewReader(io.NewSectionReader(f, from, end-from))
	offset := from
	hdr := make([]byte, recHeader)

	fail := func(reason string) error {
		if repair {
			return w.truncate(offset)
		}
		return fmt.Errorf("%w at offset %d: %s", ErrCorrupt, offset, reason)
	}

	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if err == io.EOF {
				return nil // clean end of log
			}
			return fail("partial header")
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		sum := binary.BigEndian.Uint32(hdr[4:8])
		if n == 0 || n > maxRecord {
			return fail("implausible record length")
		}

		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return fail("truncated body")
		}
		if crc32.ChecksumIEEE(body) != sum {
			return fail("checksum mismatch")
		}

		var rec Record
		if err := json.Unmarshal(body, &rec); err != nil {
			return fail("undecodable record")
		}

		// Epoch records are the log's own bookkeeping, not queue state.
		if rec.Op == OpEpoch {
			w.mu.Lock()
			if rec.Epoch > w.epoch {
				w.epoch = rec.Epoch
			}
			w.mu.Unlock()
		} else if err := fn(rec, offset); err != nil {
			return err
		}
		offset += recHeader + int64(n)
	}
}

// truncate discards a torn tail so the next append does not land after
// unreadable bytes and make the whole log unrecoverable.
func (w *WAL) truncate(at int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if at >= w.size {
		return nil
	}
	if err := w.f.Truncate(at); err != nil {
		return err
	}
	w.size = at
	return w.f.Sync()
}

// Compact rewrites the log with only the records needed to rebuild the state
// its caller snapshotted, then swaps it in atomically.
//
// The caller MUST hold every lock that could admit a new record, and must not
// release them until this returns. There is no way to make that safe here:
// any record appended between the snapshot and the rename is erased by the
// rename, and the loss is silent because the producer was already told the
// write was durable. Manager.Compact is the only intended caller and it
// enforces this by holding the manager lock and every queue lock.
func (w *WAL) Compact(live []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.failed != nil {
		return fmt.Errorf("log is unusable: %w", w.failed)
	}

	tmp := w.path + ".compacting"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	abort := func(e error) error {
		f.Close()
		os.Remove(tmp)
		return e
	}

	// The epoch record goes first, so recovery learns the generation before
	// anything else and stale bookmarks can be rejected.
	var off int64
	records := append([]Record{{Op: OpEpoch, Epoch: w.epoch + 1}}, live...)
	for _, rec := range records {
		if rec.At == 0 {
			rec.At = time.Now().UnixNano()
		}
		buf, err := encode(rec)
		if err != nil {
			return abort(err)
		}
		if _, err := f.WriteAt(buf, off); err != nil {
			return abort(err)
		}
		off += int64(len(buf))
	}

	if err := f.Sync(); err != nil {
		return abort(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		os.Remove(tmp)
		return err
	}

	// A rename is a metadata operation, so the directory entry itself has to
	// be fsynced or the swap can be lost in a crash even though the file
	// contents were durable.
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		dir.Sync()
		dir.Close()
	}

	// Past this point the old file is no longer reachable by path, so any
	// failure leaves the process unable to write to the log it thinks it
	// owns. Latch that rather than carrying on acknowledging writes into an
	// orphaned handle.
	nf, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		w.failed = fmt.Errorf("compaction swapped the log but could not reopen it: %w", err)
		return w.failed
	}

	// The previous handle is retired rather than closed, so a scan already
	// reading through it finishes against a consistent snapshot.
	w.retired = append(w.retired, w.f)
	w.f = nf
	w.size = off
	w.epoch++
	return nil
}

func encode(rec Record) ([]byte, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	if len(body) > maxRecord {
		return nil, fmt.Errorf("record too large: %d bytes", len(body))
	}
	buf := make([]byte, recHeader+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(body))
	copy(buf[recHeader:], body)
	return buf, nil
}

// Bookmark returns the current epoch and offset together. Callers must keep
// both: an offset from a previous epoch describes bytes that no longer exist.
func (w *WAL) Bookmark() (epoch uint64, offset int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.epoch, w.size
}

// Err reports a latched storage failure, or nil if the log is healthy.
func (w *WAL) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed
}

func (w *WAL) flusher() {
	defer w.wg.Done()
	t := time.NewTicker(w.syncEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.mu.Lock()
			if w.dirty {
				if err := w.f.Sync(); err != nil {
					// Writes were already acknowledged against this fsync, so
					// there is nobody left to return the error to. Latch it so
					// the next write fails loudly and health reporting can see
					// it, rather than dropping it and continuing to lie.
					if w.failed == nil {
						w.failed = err
					}
				}
				w.dirty = false
			}
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}
}

// Close stops group commit, forces a final fsync, and releases every handle.
// Safe to call more than once.
func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		if w.syncEvery > 0 {
			close(w.stop)
			w.wg.Wait()
		}
		w.mu.Lock()
		defer w.mu.Unlock()

		for _, old := range w.retired {
			old.Close()
		}
		w.retired = nil

		// Group commit may have left the last acknowledged writes in the page
		// cache, so the final fsync is not optional.
		if syncErr := w.f.Sync(); syncErr != nil {
			w.f.Close()
			err = syncErr
			return
		}
		err = w.f.Close()
	})
	return err
}
