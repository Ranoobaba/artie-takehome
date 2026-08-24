// Command demo drives the queue over HTTP and narrates what it is doing.
//
// It is deliberately a client and nothing more: it imports no queue code and
// talks only to the public API, so whatever it demonstrates is genuinely
// available to any consumer over the wire.
//
// Usage:
//
//	go run . &                 # start the server
//	go run ./cmd/demo          # run the tour
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var base string

func main() {
	flag.StringVar(&base, "addr", "http://localhost:8080", "queue server address")
	flag.Parse()

	if err := waitForServer(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot reach the queue at %s: %v\nStart it first with: go run .\n", base, err)
		os.Exit(1)
	}

	// A queue name that changes per run, so the demo is repeatable without
	// having to delete the log first.
	name := fmt.Sprintf("orders-%d", time.Now().UnixNano()%100000)

	section("1. Create a delayed priority LIFO queue")
	fmt.Println(`   The brief asks for combinations like "priority FIFO" or "delayed priority`)
	fmt.Println(`   LIFO". Those are not separate queue types here. A queue picks a tiebreak`)
	fmt.Println(`   mode and whether priority is live; delay is per message.`)
	post("/queues", map[string]any{
		"name":                  name,
		"mode":                  "lifo",
		"priority":              true,
		"max_attempts":          3,
		"visibility_timeout_ms": 1000,
	})
	fmt.Printf("   created %q: mode=lifo priority=on max_attempts=3\n", name)

	section("2. Priority outranks arrival order, LIFO breaks the ties")
	enqueue(name, "checkout-A (normal)", 1, 0)
	enqueue(name, "checkout-B (normal)", 1, 0)
	enqueue(name, "fraud-review (urgent)", 9, 0)
	enqueue(name, "checkout-C (normal)", 1, 0)
	fmt.Println("   enqueued in that order. Expected delivery: the urgent one first, then")
	fmt.Println("   the normal ones newest to oldest, because this queue is LIFO.")
	consume(name, 10)

	section("3. Delay is a separate axis, so a high priority message still waits")
	enqueue(name, "reminder-email (urgent, delayed 1.5s)", 9, 1500)
	enqueue(name, "ship-order (normal, immediate)", 1, 0)
	fmt.Println("   immediately after enqueue:")
	consume(name, 10)
	fmt.Println("   after waiting out the delay:")
	time.Sleep(1700 * time.Millisecond)
	consume(name, 10)

	section("4. A consumer that dies mid work does not lose the message")
	enqueue(name, "charge-card", 1, 0)
	got := receive(name, 1, 800, 0)
	fmt.Printf("   worker leased %q and then crashed without acking\n", got[0].Body)
	fmt.Println("   (the lease is 800ms, so nothing happens for 800ms...)")
	if empty := receive(name, 1, 800, 0); len(empty) == 0 {
		fmt.Println("   while the lease is live, no other worker can take it: confirmed")
	}
	time.Sleep(1100 * time.Millisecond)
	back := receive(name, 1, 5000, 0)
	fmt.Printf("   redelivered as attempt %d: %q\n", back[0].Attempts, back[0].Body)
	ack(name, back[0].Receipt)
	fmt.Println("   acked, so it is now permanently gone")

	fmt.Println()
	fmt.Println("   a worker that is still alive can hold its claim instead of losing it:")
	enqueue(name, "slow-report", 1, 0)
	slow := receive(name, 1, 600, 0)
	for i := 1; i <= 3; i++ {
		time.Sleep(400 * time.Millisecond)
		post("/queues/"+name+"/extend", map[string]any{
			"receipt": slow[0].Receipt, "visibility_ms": 600,
		})
		fmt.Printf("   heartbeat %d sent\n", i)
	}
	if empty := receive(name, 1, 600, 0); len(empty) == 0 {
		fmt.Println("   1.2s past the original 600ms lease and nobody else can take it")
	}
	ack(name, slow[0].Receipt)

	section("5. A poison message gives up rather than looping forever")
	enqueue(name, "malformed-payload", 1, 0)
	for i := 1; i <= 3; i++ {
		msgs := receive(name, 1, 5000, 500)
		if len(msgs) == 0 {
			break
		}
		fmt.Printf("   attempt %d failed, nacking\n", msgs[0].Attempts)
		nack(name, msgs[0].Receipt, 0)
	}
	s := stats(name)
	fmt.Printf("   max_attempts reached, so it is parked: dlq=%d ready=%d\n", s.DLQ, s.Ready)

	fmt.Println("   replaying the dead letter queue puts it back with a clean slate:")
	post("/queues/"+name+"/replay", map[string]any{"source": "dlq"})
	s = stats(name)
	fmt.Printf("   dlq=%d ready=%d\n", s.DLQ, s.Ready)
	if msgs := receive(name, 1, 5000, 500); len(msgs) > 0 {
		ack(name, msgs[0].Receipt)
	}

	section("6. Replaying history straight out of the write ahead log")
	var mark struct {
		Epoch  uint64 `json:"epoch"`
		Offset int64  `json:"offset"`
	}
	getInto("/bookmark", &mark)
	fmt.Printf("   bookmarking the log at epoch %d byte %d\n", mark.Epoch, mark.Offset)
	enqueue(name, "audit-event-1", 1, 0)
	enqueue(name, "audit-event-2", 1, 0)
	for _, m := range receive(name, 10, 5000, 0) {
		ack(name, m.Receipt)
	}
	fmt.Println("   both consumed and acked, queue is empty")
	post("/queues/"+name+"/replay", map[string]any{
		"source":      "log",
		"epoch":       mark.Epoch,
		"from_offset": mark.Offset,
	})
	fmt.Println("   replayed from the bookmark, so they are back:")
	consume(name, 10)

	section("7. Concurrency: 6 workers draining 300 messages")
	fast := fmt.Sprintf("bulk-%d", time.Now().UnixNano()%100000)
	post("/queues", map[string]any{"name": fast, "mode": "fifo", "visibility_timeout_ms": 30000})

	start := time.Now()
	var pwg sync.WaitGroup
	for p := 0; p < 6; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for i := 0; i < 50; i++ {
				enqueue(fast, fmt.Sprintf("job-%d-%d", p, i), 0, 0)
			}
		}(p)
	}
	pwg.Wait()
	produced := time.Since(start)

	var mu sync.Mutex
	seen := map[string]int{}
	start = time.Now()
	var cwg sync.WaitGroup
	for c := 0; c < 6; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				msgs := receive(fast, 10, 30000, 300)
				if len(msgs) == 0 {
					return
				}
				for _, m := range msgs {
					mu.Lock()
					seen[m.Body]++
					mu.Unlock()
					ack(fast, m.Receipt)
				}
			}
		}()
	}
	cwg.Wait()

	dupes := 0
	for _, n := range seen {
		if n > 1 {
			dupes++
		}
	}
	fmt.Printf("   produced 300 in %v (every one fsynced before its 201)\n", produced.Round(time.Millisecond))
	fmt.Printf("   consumed %d distinct in %v across 6 workers\n", len(seen), time.Since(start).Round(time.Millisecond))
	fmt.Printf("   duplicates: %d, lost: %d\n", dupes, 300-len(seen))

	section("Done")
	fmt.Printf("   Restart the server and the queues are still there.\n")
	fmt.Printf("   Inspect state any time:  curl -s %s/queues | jq\n", base)
}

// --- tiny HTTP client ---

type msg struct {
	ID       string `json:"id"`
	Body     string `json:"body"`
	Priority int    `json:"priority"`
	Seq      uint64 `json:"seq"`
	Attempts int    `json:"attempts"`
	Receipt  string `json:"receipt"`
}

type queueStats struct {
	Ready    int `json:"ready"`
	Delayed  int `json:"delayed"`
	Inflight int `json:"inflight"`
	DLQ      int `json:"dlq"`
}

func waitForServer() error {
	var last error
	for i := 0; i < 40; i++ {
		resp, err := http.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			return nil
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	return last
}

func post(path string, body any) []byte {
	buf, _ := json.Marshal(body)
	resp, err := http.Post(base+path, "application/json", bytes.NewReader(buf))
	if err != nil {
		fmt.Fprintf(os.Stderr, "POST %s: %v\n", path, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "POST %s -> %d %s\n", path, resp.StatusCode, out)
		os.Exit(1)
	}
	return out
}

func getInto(path string, dst any) {
	resp, err := http.Get(base + path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GET %s: %v\n", path, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(dst)
}

func enqueue(q, body string, priority, delayMS int) {
	post("/queues/"+q+"/messages", map[string]any{
		"body": body, "priority": priority, "delay_ms": delayMS,
	})
}

func receive(q string, max, visibilityMS, waitMS int) []msg {
	out := post("/queues/"+q+"/receive", map[string]any{
		"max": max, "visibility_ms": visibilityMS, "wait_ms": waitMS,
	})
	var wrap struct {
		Messages []msg `json:"messages"`
	}
	json.Unmarshal(out, &wrap)
	return wrap.Messages
}

func ack(q, receipt string) { post("/queues/"+q+"/ack", map[string]any{"receipt": receipt}) }

func nack(q, receipt string, delayMS int) {
	post("/queues/"+q+"/nack", map[string]any{"receipt": receipt, "delay_ms": delayMS})
}

func stats(q string) queueStats {
	var s queueStats
	getInto("/queues/"+q, &s)
	return s
}

// consume receives, prints, and acknowledges. Acking matters even in a demo:
// an unacked lease comes back when it expires, which would have later
// sections picking up messages from earlier ones.
func consume(q string, max int) {
	msgs := receive(q, max, 5000, 0)
	show(msgs)
	for _, m := range msgs {
		ack(q, m.Receipt)
	}
}

func show(msgs []msg) {
	if len(msgs) == 0 {
		fmt.Println("      (nothing available)")
		return
	}
	for i, m := range msgs {
		fmt.Printf("      %d. %-42s priority=%d seq=%d\n", i+1, m.Body, m.Priority, m.Seq)
	}
}

func section(title string) {
	fmt.Printf("\n%s\n%s\n", title, strings.Repeat("=", len(title)))
}
