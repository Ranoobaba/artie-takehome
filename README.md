# Frankenqueue

A durable HTTP message queue whose ordering is composable: FIFO or LIFO, with
or without priority, with per message delay. A "delayed priority LIFO" is not a
special mode, it is what you get by turning on two independent switches.

Storage is a write ahead log written from scratch on top of the standard
library. There is no database, no embedded key value store, and no third party
dependency anywhere in the module.

**Delivery guarantee: at least once.** A message is delivered to exactly one
consumer at a time, but it can be delivered more than once overall, because the
window between a consumer finishing its work and its acknowledgement reaching
the log cannot be made atomic. Consumers must be idempotent. The exact
interleaving that produces a duplicate is traced in
[Delivery semantics](#delivery-semantics) below.

---

## Quick start

Requires Go 1.22 or newer. Nothing else.

```bash
go run .                 # starts on :8080, log at data/queue.wal
go run ./cmd/demo        # in another shell: a narrated tour of every feature
```

The demo creates a delayed priority LIFO queue and walks through ordering,
delay, consumer crash recovery, dead lettering, log replay, and a concurrent
drain, printing what it expects and what it got at each step.

```bash
go test -race ./queue/           # full suite under the race detector
go test -bench=. -run=XXX ./queue/
```

---

## The core idea

The brief asks for FIFO, LIFO, priority, and delay, combinable. The
straightforward reading is four features. Implemented that way you get a
combinatorial mess of special cases.

They are actually **two orthogonal axes**:

| Axis | What it decides | Where it lives |
|---|---|---|
| Ordering | which eligible message goes next | one comparator |
| Eligibility | whether a message is a candidate at all | one timestamp |

Ordering is a single composite sort key, `(priority DESC, seq ASC or DESC)`:

```go
func orderBy(mode Mode) func(a, b *Msg) bool {
	return func(a, b *Msg) bool {
		if a.Priority != b.Priority {
			return a.Priority > b.Priority   // priority always outranks arrival
		}
		if mode == LIFO {
			return a.Seq > b.Seq             // newest first
		}
		return a.Seq < b.Seq                 // oldest first
	}
}
```

Delay is not in there at all. A message carries `VisibleAt`, and it is simply
not a candidate until that time passes. Because the two axes never interact,
every combination the brief asks for is the same code path with no branching:

| Configuration | Result |
|---|---|
| `mode=fifo` | plain FIFO |
| `mode=lifo` | plain LIFO |
| `mode=fifo, priority=true` | priority FIFO |
| `mode=lifo, priority=true` | priority LIFO |
| any of the above, `delay_ms` per message | the delayed variant |

`TestDelayedPriorityLIFO` asserts all three mechanics interacting at once.

---

## Architecture

```
                   ┌──────────────┐
  POST /messages ─▶│ delayed heap │   min-heap on VisibleAt
    (fsync first)  └──────┬───────┘
                          │ background pump promotes what is due
                          ▼
                   ┌──────────────┐
  POST /receive ──▶│  ready heap  │   ordered by the queue's comparator
    (lease, not    └──────┬───────┘
     a pop)               │
                          ▼
                   ┌──────────────┐
                   │inflight heap │   min-heap on LeaseExpiry
                   └───┬──────┬───┘
              ack ─────┘      └───── timeout or nack, attempts++
            (gone)                       │
                                         ├── under limit ─▶ back to ready
                                         └── over limit  ─▶ dead letter queue
```

Three heaps, one lock, one shared log.

**Why heaps and not sorted slices.** Every operation the queue performs is
either "give me the extreme element" or "insert one element", which is exactly
what a binary heap is for: `O(log n)` in, `O(log n)` out, `O(1)` to peek. The
two time driven heaps matter most. Finding expired leases by scanning every
outstanding message would be `O(n)` on every tick; with a min heap on expiry
the pump peeks at one element to decide whether there is any work at all.

**Why messages cache their heap index.** Acking a message means removing it
from the middle of the inflight heap. The receipt maps to the message pointer
in `O(1)`, the message knows its own slot, and `heap.Remove` is `O(log n)`.
Without the cached index this would be a linear search.

**Why a lease and not a pop.** A destructive read loses the message the instant
the consumer dies holding it. `receive` moves the message to the inflight heap
with an expiry; if the ack never arrives the message returns to ready with its
attempt count incremented. Each delivery mints a fresh receipt, so a consumer
holding a stale receipt from a lapsed lease cannot acknowledge work that has
since been handed to someone else (`TestStaleReceiptRejected`).

### Files

| File | Contents |
|---|---|
| `queue/msg.go` | the message, and the two fields the two axes hang off |
| `queue/heaps.go` | one heap type, three comparators, the ordering rule |
| `queue/queue.go` | a single queue: enqueue, lease, ack, nack, expiry, dead letter |
| `queue/wal.go` | the storage engine: append, fsync, CRC, recover, compact |
| `queue/manager.go` | many queues, crash recovery, replay, the background pump |
| `api.go` | HTTP handlers, standard library router only |
| `main.go` | flags, logging, graceful shutdown |
| `cmd/demo/main.go` | the demo client |

---

## Durability

The requirement was that storage could not be delegated to a separate queue or
database, so the storage engine is a write ahead log built from a file, a
length prefix, and a checksum.

**Record format.**

```
[4 bytes: body length][4 bytes: CRC32 of body][body: JSON]
```

**The write path.** `Enqueue` appends the record and fsyncs it *before* the
message enters any heap and before the handler returns 201. The ordering is the
guarantee. A crash between the fsync and the heap insert is safe, because
recovery replays the record. The reverse order would not be: we would have told
the producer we had a message that existed only in memory.

**Recovery.** On startup the log is replayed from byte zero and the heaps are
rebuilt. The rule that keeps this simple is that **a lease never survives a
restart**: any message checked out when the process died goes back to ready,
because that consumer is definitionally gone. Attempt counts do survive, so a
message that reliably kills its consumer still reaches the dead letter queue
instead of looping forever.

**Torn tails.** A process killed mid write leaves a partial record. Recovery
detects it by short read or CRC mismatch, truncates the log at the last good
record, and continues. Refusing to start would turn a routine crash into an
outage, and trusting the bytes would corrupt state.
`TestTornTailIsDiscarded` appends deliberate garbage, asserts recovery, and
then asserts the log is still writable afterwards, which is the part that is
easy to get wrong.

**Compaction.** The log is rewritten with only the records needed to rebuild
current state, then swapped in with `rename`, which is atomic on POSIX. The
directory is fsynced too, because a rename is a metadata operation and can
otherwise be lost even when the file contents are durable.

**Verified against SIGKILL, not a graceful shutdown:**

```
before crash: {"ready":3,...}
>>> SIGKILL, no flush, no cleanup <<<
after restart: {"ready":3,...}   delivery order: msg-3, msg-2, msg-1
```

Queue config, messages, priorities, and LIFO ordering all survive.

### The cost, measured

`go test -bench=. -benchtime=2000x`, Apple Silicon, Go 1.27, local SSD:

| Configuration | Per op | Throughput | What a 201 means |
|---|---|---|---|
| fsync per write (default) | 4.42 ms | ~226 msg/s | the bytes are on the disk |
| group commit, 1 ms | 82 µs | ~12,200 msg/s | on disk within 1 ms |
| group commit, 10 ms | 8.8 µs | ~113,000 msg/s | on disk within 10 ms |
| receive plus ack | 24.8 µs | ~40,000 msg/s | |
| recovery | 140 ms per 50,000 messages | ~358,000 msg/s | |

Durability costs a factor of 500. That is not a bug to optimise away, it is
what fsync costs, and it is why the knob exists rather than a default. The
default is the safe one, because the person accepting a window of possible loss
should be the person who has decided how large it can be.

Not every write pays it. Taking out a lease is appended without an fsync on
purpose: losing that record only causes a redelivery, which at least once
delivery already permits. Paying 4 ms to prevent a duplicate we are allowed to
produce anyway would be the wrong trade.

---

## Concurrency

One `sync.Mutex` per queue, plus a separate lock inside the log. Lock ordering
is always Manager, then Queue, then WAL, and never the reverse, which is why
there is no deadlock despite all three being held on some paths.

Messages returned from `Receive` are **copies**. The queue keeps mutating the
originals after the lock is released, so handing out live pointers would be a
data race, and one the race detector does catch.

Long polling parks a consumer on a channel rather than spinning. Producers and
the pump send a non blocking wake. The wait also selects on the request
context, so a consumer that hangs up frees its server goroutine immediately.

**The evidence.** `go test -race ./queue/` passes. `TestConcurrentNoLossNoDuplicates`
runs 8 producers writing 2,000 messages against 8 consumers and asserts every
message was delivered exactly once: none lost, none duplicated.
`TestConcurrentLeasesAreExclusive` asserts no message is ever leased to two
consumers at the same moment.

```
$ go test -race ./queue/
ok  	artie-takehome/queue	23.115s
```

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/queues` | create a queue |
| `GET` | `/queues` | list queues with stats |
| `GET` | `/queues/{name}` | stats for one queue |
| `POST` | `/queues/{name}/messages` | enqueue, returns after fsync |
| `POST` | `/queues/{name}/receive` | lease messages, supports long polling |
| `POST` | `/queues/{name}/ack` | confirm and delete |
| `POST` | `/queues/{name}/nack` | return to the queue, with optional backoff |
| `POST` | `/queues/{name}/replay` | replay the dead letter queue or the log |
| `POST` | `/admin/compact` | rewrite the log to live state only |
| `GET` | `/offset` | current log offset, usable as a replay bookmark |
| `GET` | `/healthz` | liveness |

```bash
curl -XPOST localhost:8080/queues \
  -d '{"name":"orders","mode":"lifo","priority":true,"max_attempts":3,"visibility_timeout_ms":30000}'

curl -XPOST localhost:8080/queues/orders/messages \
  -d '{"body":"charge card","priority":9,"delay_ms":5000}'

curl -XPOST localhost:8080/queues/orders/receive \
  -d '{"max":10,"visibility_ms":30000,"wait_ms":20000}'

curl -XPOST localhost:8080/queues/orders/ack -d '{"receipt":"..."}'
```

`GET /queues/{name}` reports ready, delayed, inflight, and dead letter depths,
lifetime counters, and the age of the oldest ready message, which is the first
number anyone asks for when a queue looks wedged.

---

## Delivery semantics

At least once. Here is the interleaving that produces a duplicate with no bug
in anyone's code:

```
consumer receives message
consumer does the work            <- the side effect has happened
consumer crashes                  <- before its ack reaches us
lease expires
message is redelivered            <- we have no way to know the work was done
```

Closing that window would require "do the work" and "acknowledge" to be one
atomic operation spanning two independent systems. That is a distributed
transaction, and in the general case it is not achievable. Every queue in this
category has the same property, and "exactly once" as a product claim means at
least once delivery plus deduplication somewhere.

**What a consumer should do.** Derive an idempotency key from the message and
make the handler safe to repeat:

```
on message m:
    if INSERT m.id INTO processed fails on the unique constraint:
        ack and return          # already handled
    do the work
    ack
```

Deliberately not built in, because the deduplication store has to live with the
side effect to be atomic with it. A dedupe table inside the queue would just
move the same window somewhere less useful.

---

## Additional questions

### How do you handle replay?

Two different things share the word, so both are implemented.

**Redelivery** is the automatic kind: a lease expires or a consumer nacks, and
the message returns to ready with an incremented attempt count. Past
`max_attempts` it is parked in the dead letter queue instead of looping.
`POST /queues/{name}/replay` with `{"source":"dlq"}` drains it back with attempt
counts reset, which is the operational move after fixing whatever was breaking.

**Historical replay** is the interesting kind, and it is available because the
storage engine is a log rather than a pile of mutable records. Every enqueue is
still sitting in the file in order. `GET /offset` returns a bookmark, and
`POST /queues/{name}/replay` with `{"source":"log","from_offset":N}` re-enqueues
everything that arrived after it. `since_unix_ms` filters by wall clock
instead.

Replayed messages are genuinely new: new IDs, new sequence numbers, zero
attempts. Resurrecting the original IDs would collide with any copy still live
in the queue and would corrupt attempt counts. Replay produces new deliveries
of old content, which is semantics a consumer can reason about.

The limit today is that compaction discards acked history, so you can only
replay back to the last compaction. Retention is currently a side effect of not
having compacted rather than a policy, which is the first thing to fix. See
[Limitations](#limitations).

### How would you refactor your queue into a Pub/Sub?

Most of the work is already done, and this is the main reason the storage
engine is a log.

A queue and a topic differ in one thing: a queue has a **destructive read** and
a topic has a **cursor**. Today an ack deletes a message, so each message is
consumed once by one consumer. In pub/sub nothing is deleted on read; each
subscription tracks its own position and reads the same underlying data
independently.

Concretely:

1. **Add a subscription registry.** A topic has N named subscriptions, each
   with a durable offset into the log. Add two record types, `OpSubscribe` and
   `OpCommitOffset`, so subscriptions recover like everything else.
2. **Change ack from delete to advance.** `Ack` currently calls `heap.Remove`.
   It becomes "set this subscription's committed offset". The message stays
   where it is.
3. **Read from the cursor instead of the heap.** `Receive` for a subscription
   scans forward from its offset. `WAL.Scan(from, fn)` already does exactly
   this and is already used by both recovery and replay.
4. **Retention becomes a policy.** Today a message lives until it is acked. It
   would live until it is older than a retention window or the log exceeds a
   size, independent of who has read it. Compaction changes from "drop acked
   records" to "drop records older than the retention window and behind every
   subscription's cursor".
5. **Fan out costs nothing.** Three subscribers to one topic is three integers,
   not three copies. This is the structural reason a log based system gets fan
   out free while SQS needs SNS in front of three separate queues.

The ordering work carries over unchanged. Per subscription delivery within a
partition stays ordered by `Seq`, and priority becomes a scheduling decision
inside a subscription's read window rather than a global reordering, since you
cannot reorder a log that other readers are also reading.

What would need real thought rather than plumbing: partitioning by key, so that
subscriptions can be consumed in parallel while preserving order per key. That
is the same problem SQS solves with `MessageGroupId`, Kafka with the partition
key, and Pulsar with `key_shared`, and the answer is the same, which is to lock
a key to one consumer while it has work in flight.

### If you had more time, what other features would you add?

Ordered by what I would actually do first.

1. **Replication.** The single largest gap. The log survives a process restart
   but not a disk. I would add a Raft group over the log, since the log already
   is a replicated state machine's state machine, and commit a record once a
   quorum has it rather than once the local disk has it. This turns durability
   from "one machine's SSD" into a real guarantee.
2. **Retention as a policy rather than an accident.** Time and size based
   retention decoupled from acks, so historical replay has a defined window.
   Prerequisite for the pub/sub work above.
3. **Partition keys.** Order per key with parallelism across keys, which is the
   only way to get both ordering and throughput. Today strict ordering and
   concurrent consumers are in tension, exactly as they are in SQS FIFO.
4. **Batch endpoints.** Batch enqueue turns N fsyncs into one, which given the
   numbers above is a factor of hundreds for bulk producers. Almost certainly
   the highest throughput return per hour of work.
5. **Move fsync off the queue lock.** The lock is currently held across the
   fsync, so one slow write stalls every other operation on that queue.
   Assigning the sequence number under the lock and committing outside it would
   let writes pipeline. Records already carry their own `Seq`, so out of order
   log entries replay correctly.
6. **Prometheus metrics and structured per message tracing.** The stats
   endpoint is a stopgap.
7. **Authentication and per queue authorization.** There is none. It is
   unauthenticated on purpose for a takehome and would be unacceptable
   anywhere else.
8. **Message TTL,** so an expired message is dropped rather than delivered late
   to a consumer that no longer cares.
9. **A snapshot format for compaction,** so recovery is a bulk load rather than
   a record by record replay. At 140 ms per 50,000 messages this is not urgent,
   but it becomes so around tens of millions.

### Why would users choose your queue over incumbents like Amazon SQS, RabbitMQ or Apache Pulsar?

For most production workloads, they should not, and I would rather say that
plainly than oversell a two hour build. SQS, RabbitMQ and Pulsar have
replication, operational tooling, client libraries in every language, and years
of production hardening. None of that is here.

There are three narrow cases where this is genuinely the better answer.

**1. The ordering combination the brief asks for does not exist in the
incumbents.** This is the real differentiator, and it is not a small one.

| | Priority | LIFO | Delay | Combined |
|---|---|---|---|---|
| SQS | none at all | no | capped at 15 minutes | no |
| RabbitMQ | yes, with caveats and a fixed level count | no | plugin | awkward |
| Pulsar | no native priority | no | yes | no |
| this | yes | yes | unbounded | yes, by construction |

A delayed priority LIFO is one line of config here. Getting it out of SQS means
priority tiers as separate queues plus a scheduler you write and operate, and
LIFO is not expressible at all. If that is the shape of your problem, this is
not a worse SQS, it is a different tool.

**2. Zero operational surface.** One binary, no cluster, no broker, no
ZooKeeper, no AWS account, no network dependency. `go run .` and you have a
durable queue. For local development, integration tests, CI, an on premise
deployment, or an edge device, standing up Pulsar is not proportionate.

**3. Legibility.** The whole storage engine is one readable file. When
durability behaviour matters and you need to know exactly what a 201 means, "it
fsyncs the record before returning, here is the line" is worth something that a
managed service cannot offer.

Against those: no replication, no HA, bounded by one machine's disk and one
lock per queue, no mature client ecosystem, no authentication, and the entire
operational burden is yours. Choose SQS if you are on AWS and want to stop
thinking about it, RabbitMQ if you need routing topologies, and Pulsar if you
need durable multi tenant streaming at scale.

---

## Limitations

Stated plainly, since the honest list is more useful than a feature list.

- **Single node, no replication.** Survives a process crash, not a disk
  failure. The most significant gap.
- **No authentication or authorization.** Any caller can do anything.
- **One lock per queue, held across the fsync.** A slow disk write blocks every
  other operation on that queue. This is the throughput ceiling and it is
  measured above rather than estimated.
- **Every message lives in memory.** The log is the durable copy, but the heaps
  hold the full working set. A very deep queue is bounded by RAM.
- **Historical replay only reaches the last compaction.** Retention is a side
  effect rather than a policy.
- **Priority is a plain integer with no starvation protection.** A steady
  stream of high priority messages will starve low priority ones indefinitely.
  Ageing, where priority rises with wait time, is the standard fix and is not
  implemented.
- **Delay resolution is bounded by the pump interval,** 50 ms by default. A
  message asking for a 10 ms delay may wait up to 60 ms.
- **`nack` backoff is whatever the consumer passes.** There is no server side
  exponential backoff policy.
- **No message size limit,** so a single enormous body could exhaust memory.
- **Group commit is all or nothing per process.** It cannot be selected per
  queue or per message, though the right design is probably a durability level
  chosen by the producer.
