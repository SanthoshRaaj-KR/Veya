package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// stream is the half of grpc.BidiStreamingServer this package uses. Naming it
// keeps the tests free of a real gRPC server: a channel-backed fake satisfies
// three methods rather than a generated interface.
type stream interface {
	Send(*pb.ServerMessage) error
	Recv() (*pb.ClientMessage, error)
	Context() context.Context
}

// session is one connected worker.
//
// It holds no durable state and is not recoverable: if the stream drops, the
// session is gone and everything it was doing fails. That is the correct
// shape, because everything durable is already in the store. A worker that
// reconnects is a new session that can be handed the same work again, and the
// ledger is what makes handing it over twice harmless.
type session struct {
	id      string
	stream  stream
	agent   string
	version string
	worker  string
	runtime string
	decides bool
	tools   map[string]core.ToolDescriptor

	// sendMu serializes writes. A gRPC stream permits one concurrent Send,
	// and the gateway deliberately issues requests concurrently, so this is
	// not optional.
	sendMu sync.Mutex

	seq     atomic.Uint64
	mu      sync.Mutex
	pending map[string]chan *pb.ClientMessage
	closed  bool
	cause   error
}

func newSession(id string, s stream, reg *pb.Register, tools map[string]core.ToolDescriptor) *session {
	return &session{
		id:      id,
		stream:  s,
		agent:   reg.GetAgentName(),
		version: reg.GetAgentVersion(),
		worker:  reg.GetWorkerId(),
		runtime: reg.GetRuntime(),
		decides: reg.GetDecides(),
		tools:   tools,
		pending: make(map[string]chan *pb.ClientMessage),
	}
}

// nextCallID returns an identifier unique within this session. It does not
// need to be unique across sessions: a result is only ever matched against the
// pending table of the session that produced it.
func (s *session) nextCallID() string {
	return fmt.Sprintf("%s.%d", s.id, s.seq.Add(1))
}

// call sends a request and waits for the matching result.
//
// The caller's context governs. A tool call inherits the worker's lease-bound
// context, so a heartbeat rejected mid-call cancels this wait and the caller
// stops working on a task that now belongs to someone else — the same
// behaviour a local Go tool gets, reached by a different route.
func (s *session) call(ctx context.Context, build func(callID string) *pb.ServerMessage) (*pb.ClientMessage, error) {
	callID := s.nextCallID()
	reply := make(chan *pb.ClientMessage, 1)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, s.closedErr()
	}
	s.pending[callID] = reply
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.pending, callID)
		s.mu.Unlock()
	}()

	if err := s.send(build(callID)); err != nil {
		return nil, fmt.Errorf("worker %s: send: %w", s.worker, err)
	}

	select {
	case msg := <-reply:
		return msg, nil

	case <-ctx.Done():
		// The request may already be executing on the other side. Saying so is
		// the honest answer, and it is what keeps an abandoned tool call
		// reconciling rather than being retried as though it never ran.
		return nil, fmt.Errorf("worker %s: %w", s.worker, ctx.Err())

	case <-s.done():
		return nil, s.closedErr()
	}
}

func (s *session) send(msg *pb.ServerMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(msg)
}

// deliver routes a result to whoever is waiting for it.
//
// A result for a call nobody is waiting on is dropped and reported. It happens
// legitimately — the caller's context expired a moment before the answer
// arrived — so it is not a fault, but it is worth seeing in a log when a tool
// is consistently answering just too late.
func (s *session) deliver(callID string, msg *pb.ClientMessage) bool {
	s.mu.Lock()
	reply, waiting := s.pending[callID]
	s.mu.Unlock()

	if !waiting {
		return false
	}
	select {
	case reply <- msg:
		return true
	default:
		// Buffered with room for exactly one, and pending is deleted after the
		// receive. A second result for one call ID is a client bug.
		return false
	}
}

// close ends the session and fails everything still waiting.
//
// Every pending caller must be woken. A tool call left blocked on a stream
// that will never answer holds a lease it cannot renew, and the task would sit
// RUNNING until the reaper noticed — recovery working correctly, for a failure
// that did not need to be recovered from.
func (s *session) close(cause error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cause = cause
	pending := s.pending
	s.pending = make(map[string]chan *pb.ClientMessage)
	s.mu.Unlock()

	for _, reply := range pending {
		close(reply)
	}
}

// done reports session termination to a waiting caller. It is derived from the
// stream's own context, so a transport-level disconnection wakes callers even
// if close has not run yet.
func (s *session) done() <-chan struct{} { return s.stream.Context().Done() }

func (s *session) closedErr() error {
	s.mu.Lock()
	cause := s.cause
	s.mu.Unlock()

	if cause == nil {
		cause = ErrWorkerGone
	}
	return fmt.Errorf("worker %s (%s): %w", s.worker, s.agent, cause)
}

// resultOf unwraps a reply, turning a nil message into the ambiguous outcome
// rather than a nil-pointer panic.
//
// A closed reply channel yields a nil message, which means the session died
// while the call was in flight. That is exactly the case that must not be read
// as "it did not happen": the worker may have completed the call and died
// before answering.
func resultOf(msg *pb.ClientMessage, err error) (*pb.ClientMessage, error) {
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, ErrWorkerGone
	}
	return msg, nil
}

// failureOf converts a reported failure, or returns nil when the result
// carried an outcome instead.
func failureOf(f *pb.Failure) error {
	if f == nil {
		return nil
	}
	return wire.Failure(f)
}

// ErrWorkerGone reports that the worker serving a call disconnected before
// answering.
//
// It is deliberately not wrapped in core.NotExecuted. A worker that vanished
// mid-call may have completed the call first, and the ledger's whole purpose
// is to treat that as the ambiguity it is.
var ErrWorkerGone = errors.New("veya: the worker disconnected before answering")
