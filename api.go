package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"artie-takehome/queue"
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
	mux.HandleFunc("POST /queues/{name}/replay", a.replay)
	mux.HandleFunc("POST /admin/compact", a.compact)
	mux.HandleFunc("GET /offset", a.offset)
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

	// This call does not return until the message is on disk and fsynced, so
	// a 201 here is a durability promise, not just an acknowledgement that
	// the bytes were parsed.
	m, err := q.Enqueue(req.Body, req.Priority, ms(req.DelayMS))
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
	if r.ContentLength > 0 && !decode(w, r, &req) {
		return
	}

	msgs, err := q.Receive(r.Context(), req.Max, ms(req.VisibilityMS), ms(req.WaitMS))
	if err != nil {
		// A cancelled request context means the consumer hung up mid long
		// poll. There is nobody left to send a response to.
		if errors.Is(err, r.Context().Err()) {
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
	if err := q.Nack(req.Receipt, ms(req.DelayMS)); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, queue.ErrNoLease) {
			status = http.StatusConflict
		}
		fail(w, status, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"nacked": true})
}

// --- replay ---

type replayReq struct {
	// Source is "dlq" to drain the dead letter queue, or "log" to re-enqueue
	// history straight out of the write ahead log.
	Source      string `json:"source"`
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
	if r.ContentLength > 0 && !decode(w, r, &req) {
		return
	}

	switch req.Source {
	case "", "dlq":
		respond(w, http.StatusOK, map[string]any{"source": "dlq", "replayed": q.ReplayDLQ()})
	case "log":
		var since time.Time
		if req.SinceUnixMS > 0 {
			since = time.UnixMilli(req.SinceUnixMS)
		}
		n, err := a.mgr.Replay(name, req.FromOffset, since)
		if err != nil {
			fail(w, http.StatusInternalServerError, err)
			return
		}
		respond(w, http.StatusOK, map[string]any{"source": "log", "replayed": n})
	default:
		fail(w, http.StatusBadRequest, errors.New(`source must be "dlq" or "log"`))
	}
}

// --- admin ---

func (a *api) compact(w http.ResponseWriter, r *http.Request) {
	before := a.mgr.Offset()
	if err := a.mgr.Compact(); err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{
		"bytes_before": before,
		"bytes_after":  a.mgr.Offset(),
	})
}

func (a *api) offset(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, map[string]any{"offset": a.mgr.Offset()})
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
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

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // a typo in a field name should be loud
	if err := dec.Decode(dst); err != nil {
		fail(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func fail(w http.ResponseWriter, status int, err error) {
	respond(w, status, map[string]string{"error": err.Error()})
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }
