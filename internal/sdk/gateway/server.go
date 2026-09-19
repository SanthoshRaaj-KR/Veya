package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// DefaultAddr is where the gateway listens when nothing says otherwise.
//
// Loopback, not 0.0.0.0. The protocol has no authentication — deliberately,
// per docs/worker-protocol.md §7 — and a port that accepts unauthenticated tool
// registrations must not be reachable from the network by default. Binding
// wider is a thing an operator does on purpose, having read that sentence.
const DefaultAddr = "127.0.0.1:50551"

// DrainTimeout bounds how long shutdown waits for in-flight calls.
//
// It has to be bounded, and the reason is specific to this protocol. A worker
// session is one stream that stays open for the life of the worker, so
// GracefulStop — which waits for every RPC to finish — waits for every
// connected worker to disconnect first, which they have no reason to do.
// Unbounded, a runtime with one idle Python worker attached never exits.
const DrainTimeout = 5 * time.Second

// Server owns the listener and the gRPC server serving one Gateway.
type Server struct {
	gw    *Gateway
	grpc  *grpc.Server
	ln    net.Listener
	log   *slog.Logger
	drain time.Duration
}

// Listen binds the address and prepares to serve.
//
// Binding happens here rather than inside Run so that a port already in use is
// a startup error the operator sees immediately, rather than a goroutine
// failing quietly a moment after the process claims to be ready.
func Listen(addr string, gw *Gateway, log *slog.Logger) (*Server, error) {
	if gw == nil {
		return nil, errors.New("gateway: a Server needs a Gateway")
	}
	if log == nil {
		log = slog.Default()
	}
	if addr == "" {
		addr = DefaultAddr
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("gateway: listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer(
		// A worker session is idle whenever the agent is idle, which for a
		// durable runtime can be hours. Without these, a NAT or load balancer
		// in between silently drops the connection and the runtime believes it
		// still has a worker until the first call times out.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		// Clients ping on the same cadence. Without permitting it, gRPC's
		// default enforcement policy answers a well-behaved client's keepalive
		// with GOAWAY, which looks exactly like the fault it was preventing.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             15 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterWorkerServer(srv, gw)

	return &Server{gw: gw, grpc: srv, ln: ln, log: log, drain: DrainTimeout}, nil
}

// Addr reports the bound address, which is how a test discovers the port when
// it asked for :0.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Gateway returns the gateway being served.
func (s *Server) Gateway() *Gateway { return s.gw }

// Run serves until ctx is cancelled, then drains and stops.
//
// Graceful first, because an in-flight tool call is an external effect that
// may be halfway through happening and killing the stream underneath it turns
// a clean shutdown into an ambiguous outcome someone has to reconcile.
// Bounded, because a worker session is a stream that stays open for the life
// of the worker: waiting for every RPC to finish means waiting for every
// worker to disconnect, which an idle one has no reason to do. Unbounded, a
// runtime with one Python worker attached simply never exits.
func (s *Server) Run(ctx context.Context) error {
	errs := make(chan error, 1)
	go func() {
		s.log.Info("worker gateway listening", "addr", s.Addr(), "version", Version)
		errs <- s.grpc.Serve(s.ln)
	}()

	select {
	case err := <-errs:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("gateway: serve: %w", err)

	case <-ctx.Done():
		s.log.Info("worker gateway stopping", "addr", s.Addr(), "drain", s.drain)
		s.drainAndStop()
		<-errs
		return nil
	}
}

// drainAndStop gives in-flight calls a bounded window, then closes everything.
func (s *Server) drainAndStop() {
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		s.grpc.GracefulStop()
	}()

	select {
	case <-drained:
	case <-time.After(s.drain):
		// The sessions still open are workers waiting for work, not calls
		// mid-flight. Closing them costs nothing: a worker that reconnects is
		// handed the same tasks again, and the ledger is what makes handing
		// them over twice harmless.
		s.log.Info("worker gateway drain timed out; closing remaining sessions",
			"after", s.drain)
		s.grpc.Stop()
		<-drained
	}
}

// Close stops the server immediately. For tests and for a failed startup,
// where there is nothing in flight worth waiting for.
func (s *Server) Close() { s.grpc.Stop() }
