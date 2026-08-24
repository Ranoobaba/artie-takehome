package queue

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkEnqueue measures the cost of the durability guarantee, which is
// the dominant cost in the whole system. The two cases are the two honest
// answers to "how durable do you want to be":
//
//	fsync-per-write   every 201 means the bytes are on the platter
//	group-commit-10ms every 201 means the bytes are in the OS, and will be on
//	                  the platter within 10ms
func BenchmarkEnqueue(b *testing.B) {
	cases := []struct {
		name string
		sync time.Duration
	}{
		{"fsync-per-write", 0},
		{"group-commit-1ms", time.Millisecond},
		{"group-commit-10ms", 10 * time.Millisecond},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			mgr, err := Open(filepath.Join(b.TempDir(), "q.wal"), tc.sync, time.Hour)
			if err != nil {
				b.Fatal(err)
			}
			defer mgr.Close()
			q, err := mgr.Create(Config{Name: "bench", Mode: FIFO})
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := q.Enqueue("a representative payload of modest size", 0, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReceiveAck measures the consume side, where the fsync is on the
// ack rather than the enqueue.
func BenchmarkReceiveAck(b *testing.B) {
	mgr, err := Open(filepath.Join(b.TempDir(), "q.wal"), 10*time.Millisecond, time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()
	q, _ := mgr.Create(Config{Name: "bench", Mode: FIFO})

	for i := 0; i < b.N; i++ {
		q.Enqueue("payload", 0, 0)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := q.Receive(ctx, 1, time.Minute, 0)
		if err != nil || len(msgs) == 0 {
			b.Fatalf("receive %d: %v", i, err)
		}
		if err := q.Ack(msgs[0].Receipt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecover measures startup time against log size, which is the cost
// that compaction exists to control.
func BenchmarkRecover(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "q.wal")

	mgr, err := Open(path, 10*time.Millisecond, time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	q, _ := mgr.Create(Config{Name: "bench", Mode: FIFO})
	for i := 0; i < 50_000; i++ {
		q.Enqueue("payload", 0, 0)
	}
	mgr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, err := Open(path, 10*time.Millisecond, time.Hour)
		if err != nil {
			b.Fatal(err)
		}
		m.Close()
	}
}
