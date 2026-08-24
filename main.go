package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"artie-takehome/queue"
)

func main() {
	var (
		addr = flag.String("addr", ":8080", "listen address")
		wal  = flag.String("wal", "data/queue.wal", "path to the write ahead log")

		// The default is the safe one. Group commit is available but has to
		// be asked for, because the person turning it on should be the person
		// who has decided how much recent work they can afford to lose.
		fsyncEvery = flag.Duration("fsync-every", 0,
			"group commit interval, e.g. 10ms. 0 means fsync inline on every durable write (safest, slowest)")

		tick = flag.Duration("tick", 50*time.Millisecond,
			"how often delay gates open and expired leases are reclaimed")

		verbose = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	start := time.Now()
	mgr, err := queue.Open(*wal, *fsyncEvery, *tick)
	if err != nil {
		log.Error("failed to open queue", "error", err)
		os.Exit(1)
	}
	log.Info("recovered from log",
		"path", *wal,
		"bytes", mgr.Offset(),
		"queues", len(mgr.List()),
		"took", time.Since(start))
	if *fsyncEvery > 0 {
		log.Warn("group commit enabled: writes acknowledged before fsync",
			"window", *fsyncEvery)
	}

	srv := &http.Server{
		Addr:    *addr,
		Handler: (&api{mgr: mgr, log: log}).routes(),

		// No WriteTimeout on purpose: a long polling receive legitimately
		// holds a response open, and a write deadline would cut it off.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Graceful shutdown. In flight leases need no special handling: they are
	// intentionally not durable, so anything checked out when we stop comes
	// back as ready on the next start rather than being stranded.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Info("listening", "addr", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-stop
	log.Info("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
	// Close last, so the final fsync happens after the last request has been
	// served rather than underneath it.
	if err := mgr.Close(); err != nil {
		log.Error("failed to close log", "error", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}
