// Package postgres delivers tasks without a broker, using the tasks table as
// the queue.
//
// It exists for two reasons. The first is practical: multi-process workers with
// nothing to install beyond the database that is already required. The second
// is that having two real dispatchers is the only honest test of whether
// core.Dispatcher is a port or a description of NATS.
//
// # Publish does nothing, and that is correct
//
// With this adapter the transport and the store are the same database. A task
// committed as PENDING is already visible to every poller, so the outbox row
// that commits alongside it *is* the publish — there is nothing left to hand
// anywhere. Marking that row published is therefore truthful rather than a
// convenient fiction.
//
// This is worth noticing about the outbox generally: it is a device for
// crossing a boundary between two systems that cannot share a transaction.
// Remove the boundary and it has no work to do.
//
// # SKIP LOCKED, and the visibility timeout underneath it
//
// FOR UPDATE SKIP LOCKED stops two pollers colliding on the same row inside one
// query. It does nothing about the gap after: the task stays PENDING until a
// worker claims it, so a poller a millisecond later would happily hand out the
// same task again.
//
// So taking a task also pushes its available_at forward. The task is not
// hidden — it is still PENDING, still visible to every query that asks what is
// true — it is only not offered again for a while. A worker that dies before
// claiming costs that delay and nothing else.
//
// The delay is a liveness knob and is allowed to be wrong in both directions:
// too short duplicates deliveries, which the conditional claim absorbs; too
// long delays recovery. Neither can corrupt anything, which is exactly why a
// timeout is an acceptable mechanism here and would not be for fencing.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Dispatcher hands out dispatchable tasks straight from the tasks table.
type Dispatcher struct {
	db    *sql.DB
	clock core.Clock

	visibility time.Duration
	minPoll    time.Duration
	maxPoll    time.Duration

	log    *slog.Logger
	closed chan struct{}
	once   sync.Once
}

// Config wires a Dispatcher. DB and Clock are required.
type Config struct {
	DB    *sql.DB
	Clock core.Clock

	// Visibility is how long a task is left un-offered after being handed to a
	// worker, giving that worker time to claim it. Defaults to 30s, matching
	// the default lease TTL: both answer "how long before we assume this
	// worker is not coming back?".
	Visibility time.Duration

	// MinPoll and MaxPoll bound the idle backoff. An idle runtime should not
	// query in a tight loop, and a busy one should not wait. Default 25ms and
	// 1s.
	MinPoll time.Duration
	MaxPoll time.Duration

	Logger *slog.Logger
}

// New validates the configuration and returns a Dispatcher.
func New(cfg Config) (*Dispatcher, error) {
	switch {
	case cfg.DB == nil:
		return nil, errors.New("dispatch/postgres: DB is required")
	case cfg.Clock == nil:
		return nil, errors.New("dispatch/postgres: Clock is required")
	}

	visibility := cfg.Visibility
	if visibility <= 0 {
		visibility = 30 * time.Second
	}
	minPoll := cfg.MinPoll
	if minPoll <= 0 {
		minPoll = 25 * time.Millisecond
	}
	maxPoll := cfg.MaxPoll
	if maxPoll <= 0 {
		maxPoll = time.Second
	}
	if maxPoll < minPoll {
		maxPoll = minPoll
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	return &Dispatcher{
		db:         cfg.DB,
		clock:      cfg.Clock,
		visibility: visibility,
		minPoll:    minPoll,
		maxPoll:    maxPoll,
		log:        log,
		closed:     make(chan struct{}),
	}, nil
}

// Publish is a no-op. See the package comment: the task row is the queue, so a
// committed task is already published.
func (d *Dispatcher) Publish(_ context.Context, _ core.TaskID) error {
	select {
	case <-d.closed:
		return core.ErrDispatcherClosed
	default:
		return nil
	}
}

// Claim blocks until a dispatchable task is found, ctx is done, or the
// dispatcher closes.
func (d *Dispatcher) Claim(ctx context.Context) (core.TaskID, error) {
	wait := d.minPoll

	for {
		select {
		case <-d.closed:
			return "", core.ErrDispatcherClosed
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		id, found, err := d.take(ctx)
		switch {
		case err != nil:
			return "", err
		case found:
			return id, nil
		}

		// Nothing to do. Back off so an idle runtime is not a tight loop, and
		// reset on the next hit so a busy one is not slow.
		select {
		case <-d.closed:
			return "", core.ErrDispatcherClosed
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(wait):
		}
		if wait *= 2; wait > d.maxPoll {
			wait = d.maxPoll
		}
	}
}

// take pulls the next dispatchable task and defers it in one statement.
//
// One statement, not a SELECT followed by an UPDATE: between those two another
// poller reads the same row, and SKIP LOCKED cannot help once the lock is gone.
// The ORDER BY matches idx_tasks_dispatchable so the ordering comes from the
// index rather than from sorting every pending task.
func (d *Dispatcher) take(ctx context.Context) (core.TaskID, bool, error) {
	now := d.clock.Now()

	var id core.TaskID
	err := d.db.QueryRowContext(ctx,
		`WITH next AS (
		     SELECT task_id
		     FROM tasks
		     WHERE status = 'PENDING' AND available_at <= $1
		     ORDER BY priority DESC, available_at, created_at
		     FOR UPDATE SKIP LOCKED
		     LIMIT 1
		 )
		 UPDATE tasks
		 SET available_at = $2
		 WHERE task_id IN (SELECT task_id FROM next)
		 RETURNING task_id`,
		now, now.Add(d.visibility)).Scan(&id)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", false, err
		}
		if errors.Is(err, sql.ErrConnDone) {
			return "", false, core.ErrDispatcherClosed
		}
		return "", false, fmt.Errorf("dispatch/postgres: take next task: %w", err)
	}
	return id, true, nil
}

// Close stops delivery. The pool is not closed here: it belongs to the store,
// which the composition root closes separately. Safe to call more than once.
func (d *Dispatcher) Close() error {
	d.once.Do(func() { close(d.closed) })
	return nil
}
