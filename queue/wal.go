package queue

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Op is the kind of mutation a log record describes. The set is deliberately
// small: every state transition in the queue is one of these seven.
type Op string

const (
	OpCreateQueue Op = "create_queue"
	OpEnqueue     Op = "enqueue"
	OpLease       Op = "lease"
	OpAck         Op = "ack"
	OpNack        Op = "nack"
	OpDLQ         Op = "dlq"
	OpReplay      Op = "replay"
)

// Record is one entry in the write ahead log.
type Record struct {
	Op     Op      `json:"op"`
	Queue  string  `json:"q,omitempty"`
	ID     string  `json:"id,omitempty"`
	Msg    *Msg    `json:"m,omitempty"`
	Config *Config `json:"cfg,omitempty"`

	// At is the wall clock time the record was written, in unix nanoseconds.
	// It is what makes replay by timestamp possible.
	At int64 `json:"at"`
}

const (
	// recHeader is 4 bytes of body length followed by 4 bytes of CRC32.
	recHeader = 8

	// maxRecord bounds how much we will trust a length prefix. Without it, a
	// corrupt length field could make us try to allocate an arbitrary amount
	// of memory during recovery.
	maxRecord = 32 << 20
)

// WAL is an append only write ahead log, and it is the entire storage engine.
//
// The requirement was that storage could not be delegated to a separate queue
// or database, so this is deliberately built from the standard library alone:
// a file, a length prefix, and a checksum. Durability comes from appending a
// record and fsyncing it before the operation is acknowledged, and recovery
// comes from replaying the file from the beginning and rebuilding the heaps.
//
// Two properties make that safe:
//
//   - Records are self describing and checksummed, so a record that was only
//     partially written when the process died is detected rather than parsed
//     as garbage.
//   - Writes go through WriteAt at a tracked offset rather than through the
//     file's own cursor, so a concurrent reader can seek freely without ever
//     disturbing a writer.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	size int64

	// syncEvery == 0 means fsync inline on every durability critical append,
	// which is the safe default. A positive value enables group commit:
	// appends return once the bytes reach the OS, and a background goroutine
	// fsyncs on this interval. That trades a bounded window of possible loss
	// for a large throughput gain, and it is a knob rather than a default
	// because the right answer depends on the caller's tolerance.
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

// AppendSync writes a record and does not return until it is durable. Used
// for enqueue and ack, where loss would mean either dropping a message the
// producer was told we had, or re-running work the consumer already finished.
func (w *WAL) AppendSync(rec Record) error { return w.write(rec, true) }

func (w *WAL) write(rec Record, durable bool) error {
	if rec.At == 0 {
		rec.At = time.Now().UnixNano()
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(body) > maxRecord {
		return fmt.Errorf("record too large: %d bytes", len(body))
	}

	buf := make([]byte, recHeader+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(body))
	copy(buf[recHeader:], body)

	w.mu.Lock()
	defer w.mu.Unlock()

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
	return w.f.Sync()
}

// Scan reads records starting at a byte offset and hands each to fn along
// with its own offset.
//
// One function serves both jobs the log has to do: recovery on startup calls
// it with from = 0, and the historical replay endpoint calls it with a later
// offset.
//
// The lock is taken only to sample the current end of the log, and is then
// released before any reading happens. That matters for more than
// performance: fn is allowed to write back into the queue, which means fn can
// re-enter the WAL, and holding the lock across the callback would deadlock.
func (w *WAL) Scan(from int64, fn func(Record, int64) error) error {
	w.mu.Lock()
	end := w.size
	w.mu.Unlock()

	if from >= end {
		return nil
	}

	r := bufio.NewReader(io.NewSectionReader(w.f, from, end-from))
	offset := from
	hdr := make([]byte, recHeader)

	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if err == io.EOF {
				return nil // clean end of log
			}
			// A partial header means the process died mid write.
			return w.truncate(offset)
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		sum := binary.BigEndian.Uint32(hdr[4:8])
		if n == 0 || n > maxRecord {
			return w.truncate(offset)
		}

		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return w.truncate(offset)
		}
		if crc32.ChecksumIEEE(body) != sum {
			// The bytes are there but they are not what we wrote, so this is
			// the tail of an interrupted write. Everything before it is good.
			return w.truncate(offset)
		}

		var rec Record
		if err := json.Unmarshal(body, &rec); err != nil {
			return w.truncate(offset)
		}
		if err := fn(rec, offset); err != nil {
			return err
		}
		offset += recHeader + int64(n)
	}
}

// truncate discards a torn tail so that the next append does not land after
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

// Compact rewrites the log so that it contains only the records needed to
// rebuild current state, then atomically swaps it into place.
//
// Without this the log grows without bound and startup gets slower forever,
// which is the obvious objection to a log structured design. The swap is a
// rename, which is atomic on POSIX, so a crash at any point leaves either the
// old complete log or the new complete log, never a half written one.
func (w *WAL) Compact(live []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmp := w.path + ".compacting"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	var off int64
	for _, rec := range live {
		if rec.At == 0 {
			rec.At = time.Now().UnixNano()
		}
		body, err := json.Marshal(rec)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		buf := make([]byte, recHeader+len(body))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
		binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(body))
		copy(buf[recHeader:], body)

		if _, err := f.WriteAt(buf, off); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		off += int64(len(buf))
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		os.Remove(tmp)
		return err
	}

	// Renames are metadata operations, so the directory entry itself has to
	// be fsynced or the swap can be lost in a crash even though the file
	// contents were durable.
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		dir.Sync()
		dir.Close()
	}

	old := w.f
	nf, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.f = nf
	w.size = off
	old.Close()
	return nil
}

// Size reports the current length of the log in bytes, which is also the
// offset any future record will be written at. The replay endpoint uses it as
// a bookmark.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
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
				w.f.Sync()
				w.dirty = false
			}
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}
}

// Close stops group commit, forces a final fsync, and releases the file. It
// is safe to call more than once.
func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		if w.syncEvery > 0 {
			close(w.stop)
			w.wg.Wait()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		// The final fsync matters: group commit may have left the last few
		// acknowledged writes sitting in the page cache.
		if syncErr := w.f.Sync(); syncErr != nil {
			w.f.Close()
			err = syncErr
			return
		}
		err = w.f.Close()
	})
	return err
}
