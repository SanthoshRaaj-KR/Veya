// Package memory implements core.Store entirely in process.
//
// It is not a toy. It runs the same contract suite as the PostgreSQL adapter
// (internal/core/storetest), which is what makes it usable as the substrate
// for fast engine tests and, from Layer 7, for the deterministic simulation
// harness. If a behaviour works here but not on PostgreSQL, or the reverse,
// one of the two adapters is wrong and the suite is what says so.
//
// Writing this adapter first is deliberate. A port method that is awkward to
// implement in a map is usually a port shaped around what SQL made easy, and
// it gets reworded in domain terms before the real adapter is written.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Store keeps all state in memory, guarded by a single mutex.
//
// Transactions are copy-on-write: a transaction mutates a clone and the clone
// is swapped in only on commit, so a failed transaction leaves nothing behind.
// Holding the lock for the whole transaction makes writes fully serialized,
// which is stricter than PostgreSQL's default isolation. That is a safe
// direction to differ in — a caller correct under serialization is correct
// under a weaker level too, and the contract suite asserts the conflicts that
// matter are still reported rather than silently avoided.
type Store struct {
	mu    sync.RWMutex
	st    *state
	clock core.Clock
}

// New returns an empty Store.
func New(clk core.Clock) *Store {
	return &Store{st: newState(), clock: clk}
}

type stepKey struct {
	run  core.RunID
	step core.StepID
}

type state struct {
	runs      map[core.RunID]core.Run
	tasks     map[core.TaskID]core.Task
	stepIndex map[stepKey]core.TaskID // enforces UNIQUE (run_id, step_id)
	events    map[core.RunID][]core.Event
	effects   map[core.IdempotencyKey]core.Effect // enforces UNIQUE (idempotency_key)
	leases    map[core.TaskID]core.Lease
	workers   map[string]core.Worker
}

func newState() *state {
	return &state{
		runs:      map[core.RunID]core.Run{},
		tasks:     map[core.TaskID]core.Task{},
		stepIndex: map[stepKey]core.TaskID{},
		events:    map[core.RunID][]core.Event{},
		effects:   map[core.IdempotencyKey]core.Effect{},
		leases:    map[core.TaskID]core.Lease{},
		workers:   map[string]core.Worker{},
	}
}

func (s *state) clone() *state {
	c := &state{
		runs:      make(map[core.RunID]core.Run, len(s.runs)),
		tasks:     make(map[core.TaskID]core.Task, len(s.tasks)),
		stepIndex: make(map[stepKey]core.TaskID, len(s.stepIndex)),
		events:    make(map[core.RunID][]core.Event, len(s.events)),
		effects:   make(map[core.IdempotencyKey]core.Effect, len(s.effects)),
		leases:    make(map[core.TaskID]core.Lease, len(s.leases)),
		workers:   make(map[string]core.Worker, len(s.workers)),
	}
	for k, v := range s.runs {
		c.runs[k] = v
	}
	for k, v := range s.tasks {
		c.tasks[k] = v
	}
	for k, v := range s.stepIndex {
		c.stepIndex[k] = v
	}
	for k, v := range s.events {
		c.events[k] = append([]core.Event(nil), v...)
	}
	for k, v := range s.effects {
		c.effects[k] = v
	}
	for k, v := range s.leases {
		c.leases[k] = v
	}
	for k, v := range s.workers {
		c.workers[k] = v
	}
	return c
}

// RunInTx runs fn against a private copy of the state and publishes it only if
// fn returns nil.
func (s *Store) RunInTx(ctx context.Context, fn func(context.Context, core.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	working := s.st.clone()
	if err := fn(ctx, &tx{st: working, clock: s.clock}); err != nil {
		return err // clone discarded; nothing observable changed
	}
	s.st = working
	return nil
}

func (s *Store) GetRun(_ context.Context, id core.RunID) (core.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.st.runs[id]
	if !ok {
		return core.Run{}, fmt.Errorf("run %s: %w", id, core.ErrNotFound)
	}
	return r, nil
}

func (s *Store) GetTask(_ context.Context, id core.TaskID) (core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.st.tasks[id]
	if !ok {
		return core.Task{}, fmt.Errorf("task %s: %w", id, core.ErrNotFound)
	}
	return t, nil
}

func (s *Store) ListTasks(_ context.Context, runID core.RunID) ([]core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Task
	for _, t := range s.st.tasks {
		if t.RunID == runID {
			out = append(out, t)
		}
	}
	sortTasks(out)
	return out, nil
}

func (s *Store) History(_ context.Context, runID core.RunID) ([]core.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evs := append([]core.Event(nil), s.st.events[runID]...)
	sort.Slice(evs, func(i, j int) bool { return evs[i].Seq < evs[j].Seq })
	return evs, nil
}

func (s *Store) PendingTasks(_ context.Context, limit int) ([]core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Task
	for _, t := range s.st.tasks {
		if t.Status == core.TaskPending {
			out = append(out, t)
		}
	}
	sortTasks(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) RunsAwaitingAdvance(_ context.Context, limit int) ([]core.RunID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inFlight := map[core.RunID]bool{}
	for _, t := range s.st.tasks {
		if !t.Status.IsTerminal() {
			inFlight[t.RunID] = true
		}
	}

	var out []core.RunID
	for id, r := range s.st.runs {
		if r.Status == core.RunRunning && !inFlight[id] {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) Close() error { return nil }

func sortTasks(ts []core.Task) {
	sort.Slice(ts, func(i, j int) bool {
		if !ts[i].CreatedAt.Equal(ts[j].CreatedAt) {
			return ts[i].CreatedAt.Before(ts[j].CreatedAt)
		}
		return ts[i].ID < ts[j].ID
	})
}

// tx is the write surface over a private copy of the state.
type tx struct {
	st    *state
	clock core.Clock
}

func (t *tx) GetTask(_ context.Context, id core.TaskID) (core.Task, error) {
	task, ok := t.st.tasks[id]
	if !ok {
		return core.Task{}, fmt.Errorf("task %s: %w", id, core.ErrNotFound)
	}
	return task, nil
}

func (t *tx) CreateRun(_ context.Context, r core.Run) error {
	if _, exists := t.st.runs[r.ID]; exists {
		return fmt.Errorf("run %s: %w", r.ID, core.ErrConflict)
	}
	if !r.Status.Valid() {
		return fmt.Errorf("run %s status %q: %w", r.ID, r.Status, core.ErrInvalidTransition)
	}
	now := t.clock.Now()
	r.CreatedAt, r.UpdatedAt = now, now
	t.st.runs[r.ID] = r
	return nil
}

func (t *tx) AdvanceRun(_ context.Context, id core.RunID, expectedVersion int64, next core.RunState) error {
	r, ok := t.st.runs[id]
	if !ok {
		return fmt.Errorf("run %s: %w", id, core.ErrNotFound)
	}
	// The compare half of compare-and-swap. A loser here re-reads and finds
	// the work already done; it is not an error condition.
	if r.Version != expectedVersion {
		return fmt.Errorf("run %s at version %d, expected %d: %w",
			id, r.Version, expectedVersion, core.ErrConflict)
	}
	if next.Status != r.Status && !r.Status.CanTransitionTo(next.Status) {
		return fmt.Errorf("run %s %s -> %s: %w", id, r.Status, next.Status, core.ErrInvalidTransition)
	}

	now := t.clock.Now()
	r.Status = next.Status
	r.Output = next.Output
	r.LastError = next.LastError
	r.Version++
	r.UpdatedAt = now
	if next.Status.IsTerminal() {
		done := now
		r.CompletedAt = &done
	}
	t.st.runs[id] = r
	return nil
}

func (t *tx) CreateTask(_ context.Context, task core.Task) error {
	if _, ok := t.st.runs[task.RunID]; !ok {
		return fmt.Errorf("run %s: %w", task.RunID, core.ErrNotFound)
	}
	// UNIQUE (run_id, step_id). This is what makes task creation idempotent:
	// re-deriving the same step of the same run cannot produce a second task,
	// and the constraint is what guarantees it rather than a prior read.
	key := stepKey{run: task.RunID, step: task.StepID}
	if _, exists := t.st.stepIndex[key]; exists {
		return fmt.Errorf("run %s step %s: %w", task.RunID, task.StepID, core.ErrTaskExists)
	}
	if _, exists := t.st.tasks[task.ID]; exists {
		return fmt.Errorf("task %s: %w", task.ID, core.ErrConflict)
	}

	now := t.clock.Now()
	task.CreatedAt, task.UpdatedAt = now, now
	t.st.tasks[task.ID] = task
	t.st.stepIndex[key] = task.ID
	return nil
}

func (t *tx) TransitionTask(_ context.Context, id core.TaskID, from, to core.TaskStatus, out core.TaskOutcome) error {
	task, ok := t.st.tasks[id]
	if !ok {
		return fmt.Errorf("task %s: %w", id, core.ErrNotFound)
	}
	// Conditional on the current status. Two workers handed the same task
	// both try PENDING -> RUNNING; exactly one sees a match.
	if task.Status != from {
		return fmt.Errorf("task %s is %s, expected %s: %w", id, task.Status, from, core.ErrConflict)
	}
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("task %s %s -> %s: %w", id, from, to, core.ErrInvalidTransition)
	}

	now := t.clock.Now()
	task.Status = to
	task.UpdatedAt = now
	if out.Error != "" {
		task.LastError = out.Error
	}
	if to == core.TaskRunning {
		task.Attempt++
	}
	if to.IsTerminal() {
		done := now
		task.CompletedAt = &done
	}
	t.st.tasks[id] = task
	return nil
}

func (t *tx) NextSeq(_ context.Context, runID core.RunID) (int64, error) {
	if _, ok := t.st.runs[runID]; !ok {
		return 0, fmt.Errorf("run %s: %w", runID, core.ErrNotFound)
	}
	var max int64
	for _, e := range t.st.events[runID] {
		if e.Seq > max {
			max = e.Seq
		}
	}
	return max + 1, nil
}

func (t *tx) AppendEvent(_ context.Context, e core.Event) error {
	if _, ok := t.st.runs[e.RunID]; !ok {
		return fmt.Errorf("run %s: %w", e.RunID, core.ErrNotFound)
	}
	// PRIMARY KEY (run_id, seq). The append is conditional on the caller's
	// belief about what comes next, so a racing retry collides instead of
	// duplicating, and history stays gapless.
	for _, existing := range t.st.events[e.RunID] {
		if existing.Seq == e.Seq {
			return fmt.Errorf("run %s seq %d: %w", e.RunID, e.Seq, core.ErrSeqConflict)
		}
	}
	e.CreatedAt = t.clock.Now()
	t.st.events[e.RunID] = append(t.st.events[e.RunID], e)
	return nil
}
