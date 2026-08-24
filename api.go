package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"artie-takehome/queue"
)

// Bounds on client supplied durations and batch sizes. Every one of these is
// a value an unauthenticated caller controls, so each needs a ceiling: an
// unbounded wait pins a goroutine and a socket for as long as the caller
// likes, and an unbounded delay overflows into the past and delivers a
// message immediately, which is the opposite of what was asked for.
const (
	maxDelay      = 365 * 24 * time.Hour
	maxVisibility = 12 * time.Hour
	maxWait       = 60 * time.Second
	maxBatch      = 1000
)

// api is the HTTP surface over the queue manager. It holds no state of its
// own: every handler is a thin translation between JSON and a manager call,
// which keeps all the interesting behaviour testable without a server.
type api struct {
	mgr *queue.Manager
	log *slog.Logger
}

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()

	// Method and path patterns come from the standard library router, so
	// there is no third party dependency in the whole program.
	mux.HandleFunc("POST /queues", a.createQueue)
	mux.HandleFunc("GET /queues", a.listQueues)
	mux.HandleFunc("GET /queues/{name}", a.queueStats)
	mux.HandleFunc("POST /queues/{name}/messages", a.enqueue)
	mux.HandleFunc("POST /queues/{name}/receive", a.receive)
	mux.HandleFunc("POST /queues/{name}/ack", a.ack)
	mux.HandleFunc("POST /queues/{name}/nack", a.nack)
	mux.HandleFunc("POST /queues/{name}/extend", a.extend)
	mux.HandleFunc("POST /queues/{name}/replay", a.replay)
	mux.HandleFunc("POST /admin/compact", a.compact)
	mux.HandleFunc("GET /bookmark", a.bookmark)
	mux.HandleFunc("GET /healthz", a.health)

	return a.withLogging(mux)
}

// --- queues ---

func (a *api) createQueue(w http.ResponseWriter, r *http.Request) {
	var cfg queue.Config
	if !decode(w, r, &cfg) {
		return
	}
	if cfg.Mode == "" {
		cfg.Mode = queue.FIFO
	}
	if cfg.VisibilityTimeoutMS < 0 || time.Duration(cfg.VisibilityTimeoutMS)*time.Millisecond > maxVisibility {
		fail(w, http.StatusBadRequest, fmt.Errorf("visibility_timeout_ms must be between 0 and %d", maxVisibility/time.Millisecond))
		return
	}
	q, err := a.mgr.Create(cfg)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, queue.ErrQueueExists) {
			status = http.StatusConflict
		}
		fail(w, status, err)
		return
	}
	respond(w, http.StatusCreated, q.Config())
}

func (a *api) listQueues(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, map[string]any{"queues": a.mgr.List()})
}

func (a *api) queueStats(w http.ResponseWriter, r *http.Request) {
	q, err := a.mgr.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	respond(w, http.StatusOK, q.Stats())
}

// --- messages ---

type enqueueReq struct {
	Body     string `json:"body"`
	Priority int    `json:"priority"`
	DelayMS  int    `json:"delay_ms"`
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	q, err := a.mgr.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req enqueueReq
	if !decode(w, r, &req) {
		return
	}
	if req.Body == "" {
		fail(w, http.StatusBadRequest, errors.New("body is required"))
		return
	}
	delay, err := boundedMS(req.DelayMS, maxDelay, "delay_ms")
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	// This call does not return until the message is on disk and fsynced, so
	// a 201 here is a durability promise, not just an acknowledgement that
	// the bytes were parsed.
	m, err := q.Enqueue(req.Body, req.Priority, delay)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, queue.ErrPriorityDisabled) {
			status = http.StatusBadRequest
		}
		fail(w, status, err)
		return
	}
	respond(w, http.StatusCreated, m)
}

type receiveReq struct {
	Max          int `json:"max"`
	VisibilityMS int `json:"visibility_ms"`
	WaitMS       int `json:"wait_ms"`
}

func (a *api) receive(w http.ResponseWriter, r *http.Request) {
	q, err := a.mgr.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req receiveReq
	if !decode(w, r, &req) {
		return
	}
	if req.Max < 0 || req.Max > maxBatch {
		fail(w, http.StatusBadRequest, fmt.Errorf("max must be between 0 and %d", maxBatch))
		return
	}
	visibility, err := boundedMS(req.VisibilityMS, maxVisibility, "visibility_ms")
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	wait, err := boundedMS(req.WaitMS, maxWait, "wait_ms")
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	msgs, err := q.Receive(r.Context(), req.Max, visibility, wait)
	if err != nil {
		// A cancelled request context means the consumer hung up mid long
		// poll. There is nobody left to send a response to.
		if r.Context().Err() != nil {
			return
		}
		fail(w, http.StatusInternalServerError, err)
		return
	}
	if msgs == nil {
		msgs = []queue.Msg{}
	}
	respond(w, http.StatusOK, map[string]any{"messages": msgs})
}

type ackReq struct {
	Receipt string `json:"receipt"`
}

func (a *api) ack(w http.ResponseWriter, r *http.Request) {
	q, err := a.mgr.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req ackReq
	if !decode(w, r, &req) {
		return
	}
	if err := q.Ack(req.Receipt); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, queue.ErrNoLease) {
			// 409 rather than 404: the message may well exist, but this
			// caller's claim on it has lapsed, which is a conflict.
			status = http.StatusConflict
		}
		fail(w, status, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"acked": true})
}

type nackReq struct {
	Receipt string `json:"receipt"`
	DelayMS int    `json:"delay_ms"`
}

func (a *api) nack(w http.ResponseWriter, r *http.Request) {
	q, err := a.mgr.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req nackReq
	if !decode(w, r, &req) {
		return
	}
	delay, err := boundedMS(req.DelayMS, maxDelay, "delay_ms")
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	if err := q.Nack(req.Receipt, delay); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, queue.ErrNoLease) {
			status = http.StatusConflict
		}
		fail(w, status, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"nacked": true})
}

type extendReq struct {
	Receipt      string `json:"receipt"`
	VisibilityMS int    `json:"visibility_ms"`
}

// extend is the heartbeat a long running consumer sends to keep its claim.
// SQS calls the same operation ChangeMessageVisibility.
func (a *api) extend(w http.ResponseWriter, r *http.Request) {
	q, err := a.mgr.Get(r.PathValue("name"))
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req extendReq
	if !decode(w, r, &req) {
		return
	}
	visibility, err := boundedMS(req.VisibilityMS, maxVisibility, "visibility_ms")
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	expiry, err := q.Extend(req.Receipt, visibility)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, queue.ErrNoLease) {
			// The lease already lapsed and somebody else may hold the message
			// now, so this caller has to stop working on it.
			status = http.StatusConflict
		}
		fail(w, status, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"lease_expires_at": expiry})
}

// --- replay ---

type replayReq struct {
	// Source is "dlq" to drain the dead letter queue, or "log" to re-enqueue
	// history straight out of the write ahead log.
	Source      string `json:"source"`
	Epoch       uint64 `json:"epoch"`
	FromOffset  int64  `json:"from_offset"`
	SinceUnixMS int64  `json:"since_unix_ms"`
}

func (a *api) replay(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	q, err := a.mgr.Get(name)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	var req replayReq
	if !decode(w, r, &req) {
		return
	}

	switch req.Source {
	case "", "dlq":
		n, err := q.ReplayDLQ()
		if err != nil {
			// The count is reported alongside the error, because everything
			// counted is already live and a blind retry would duplicate it.
			respond(w, http.StatusInternalServerError, map[string]any{
				"source": "dlq", "replayed": n, "error": err.Error(),
			})
			return
		}
		respond(w, http.StatusOK, map[string]any{"source": "dlq", "replayed": n})

	case "log":
		if req.FromOffset < 0 {
			fail(w, http.StatusBadRequest, errors.New("from_offset must not be negative"))
			return
		}
		var since time.Time
		if req.SinceUnixMS > 0 {
			since = time.UnixMilli(req.SinceUnixMS)
		}
		n, err := a.mgr.Replay(name, queue.Bookmark{Epoch: req.Epoch, Offset: req.FromOffset}, since)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, queue.ErrStaleBookmark) || errors.Is(err, queue.ErrCorrupt) {
				status = http.StatusBadRequest
			}
			respond(w, status, map[string]any{
				"source": "log", "replayed": n, "error": err.Error(),
			})
			return
		}
		respond(w, http.StatusOK, map[string]any{"source": "log", "replayed": n})

	default:
		fail(w, http.StatusBadRequest, errors.New(`source must be "dlq" or "log"`))
	}
}

// --- admin ---

func (a *api) compact(w http.ResponseWriter, r *http.Request) {
	before := a.mgr.Bookmark()
	if err := a.mgr.Compact(); err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	after := a.mgr.Bookmark()
	respond(w, http.StatusOK, map[string]any{
		"bytes_before": before.Offset,
		"bytes_after":  after.Offset,
		"epoch":        after.Epoch,
		"note":         "bookmarks issued before this compaction are no longer valid",
	})
}

func (a *api) bookmark(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, a.mgr.Bookmark())
}

// health reports the state of the storage engine rather than the liveness of
// the process. A server whose log has failed is still answering requests, and
// a static "ok" would hide exactly the condition an operator needs to see.
func (a *api) health(w http.ResponseWriter, r *http.Request) {
	if err := a.mgr.Err(); err != nil {
		respond(w, http.StatusServiceUnavailable, map[string]any{
			"status": "degraded",
			"error":  err.Error(),
		})
		return
	}
	respond(w, http.StatusOK, map[string]any{"status": "ok"})
}

// --- helpers ---

func (a *api) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		a.log.Debug("request",
			"method", r.Method,
			"path", r.URL.Path,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

// decode reads a JSON body, treating an absent body as an empty object.
//
// It deliberately does not test ContentLength first. Go reports -1 for any
// chunked or streaming body, so gating on a positive length silently drops
// the whole request and dispatches the zero value instead, which on the
// replay endpoint means running a completely different operation than the
// caller asked for.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // a typo in a field name should be loud
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return true // no body at all, defaults apply
		}
		fail(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

// boundedMS converts milliseconds to a duration, rejecting negatives and
// anything past the ceiling. The comparison happens in milliseconds, before
// the multiply, so an enormous value is rejected rather than overflowing into
// a negative duration.
func boundedMS(n int, max time.Duration, field string) (time.Duration, error) {
	if n < 0 {
		return 0, fmt.Errorf("%s must not be negative", field)
	}
	limit := int64(max / time.Millisecond)
	if int64(n) > limit {
		return 0, fmt.Errorf("%s must be at most %d ms", field, limit)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func fail(w http.ResponseWriter, status int, err error) {
	respond(w, status, map[string]string{"error": err.Error()})
}
