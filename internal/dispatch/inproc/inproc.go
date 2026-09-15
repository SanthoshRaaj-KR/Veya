// Package inproc is the simplest possible core.Dispatcher: a buffered channel
// between an engine and a worker in the same process.
//
// It is deliberately the dumbest implementation that satisfies the port. Layer
// 3 adds the JetStream adapter, and the measure of whether the port was
// designed correctly is that doing so touches this package's siblings and
// cmd/, but nothing in engine/ or core/.
//
// # What it does not do
//
// No acknowledgement, no redelivery, no visibility timeout. That is honest
// rather than lazy: a message here cannot be lost in transit because it never
// leaves the process, so the machinery that exists to survive transit would be
// decoration.
//
// It can still drop work — if the process dies, the channel dies with it. That
// is survivable for a different reason: the task is committed in PostgreSQL as
// PENDING, and the runtime's pending-task scan republishes anything the
// channel never delivered. Delivery is at-least-once here as everywhere else,
// and the conditional claim is what makes duplicates safe.
package inproc

import (
	"context"
	"sync"

	"github.com/santhoshraajkr/veya/internal/core"
)

// Dispatcher hands task identities from publisher to claimer over a channel.
type Dispatcher struct {
	ch     chan core.TaskID
	closed chan struct{}
	once   sync.Once
}

// New returns a dispatcher with the given buffer depth.
func New(buffer int) *Dispatcher {
	if buffer < 1 {
		buffer = 1
	}
	return &Dispatcher{
		ch:     make(chan core.TaskID, buffer),
		closed: make(chan struct{}),
	}
}

// Publish makes a committed task available for delivery.
//
// It carries the identity only. The claimer reads what is actually true from
// the store, which is why a duplicate or out-of-order delivery carries no
// information that could be wrong.
func (d *Dispatcher) Publish(ctx context.Context, id core.TaskID) error {
	select {
	case <-d.closed:
		return core.ErrDispatcherClosed
	default:
	}

	select {
	case d.ch <- id:
		return nil
	case <-d.closed:
		return core.ErrDispatcherClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Claim blocks until a task arrives, ctx is done, or the dispatcher closes.
func (d *Dispatcher) Claim(ctx context.Context) (core.TaskID, error) {
	select {
	// Drain buffered work before reporting closure, so a shutdown does not
	// strand tasks that were already published.
	case id := <-d.ch:
		return id, nil
	default:
	}

	select {
	case id := <-d.ch:
		return id, nil
	case <-d.closed:
		return "", core.ErrDispatcherClosed
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Close stops further delivery. Safe to call more than once.
func (d *Dispatcher) Close() error {
	d.once.Do(func() { close(d.closed) })
	return nil
}
