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

// Server owns the listener and the gRPC server serving one Gateway.
type Server struct {
	gw   *Gateway
	grpc *grpc.Server
	ln   net.Listener
	log  *slog.Logger
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

	return &Server{gw: gw, grpc: srv, ln: ln, log: log}, nil
}

// Addr reports the bound address, which is how a test discovers the port when
// it asked for :0.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Gateway returns the gateway being served.
func (s *Server) Gateway() *Gateway { return s.gw }

// Run serves until ctx is cancelled, then stops gracefully.
//
// GracefulStop rather than Stop: an in-flight tool call is an external effect
// that may be halfway through happening, and killing the stream underneath it
// turns a clean shutdown into an ambiguous outcome that has to be reconciled.
// Waiting costs seconds at shutdown and saves an UNKNOWN effect per in-flight
// call.
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
		s.log.Info("worker gateway stopping", "addr", s.Addr())
		s.grpc.GracefulStop()
		<-errs
		return nil
	}
}

// Close stops the server immediately. For tests and for a failed startup,
// where there is nothing in flight worth waiting for.
func (s *Server) Close() { s.grpc.Stop() }
