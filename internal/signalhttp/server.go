// Package signalhttp is the HTTP door onto engine.DeliverSignal.
//
// `veya signal` proved the port: a signal is stored on arrival whether or not
// anything is waiting, so early and late arrival run identical code, and the
// only two writes involved are RecordSignal and ReleaseRun, in one
// transaction. This package adds no third write path. It is a handler that
// parses a request, builds a core.Signal, and calls the same Engine.Signal
// the CLI's DeliverSignal sits underneath — the seam status.md section 5.1
// named before this file existed.
//
// # Auth, deliberately not answered here either
//
// Same posture as internal/sdk/gateway, for the identical reason. DefaultAddr
// binds loopback, not 0.0.0.0, and there is no authentication: a port that
// accepts an unauthenticated "this run should proceed" is not safe to expose
// to a network by default. Binding wider, or putting a reverse proxy in front
// that adds auth, is an operator decision this package does not make for
// them — it is not half-built, it is scoped the same way the worker gateway
// already is.
package signalhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
)

// DefaultAddr is where the server listens when nothing says otherwise.
//
// Loopback, per the package doc. Distinct from the worker gateway's default
// port (50551) and from every other port this project reserves — see the
// environment table in docs/status.md.
const DefaultAddr = "127.0.0.1:8089"

// ShutdownTimeout bounds how long Run waits for an in-flight request to
// finish once its context is cancelled.
//
// A signal delivery is one request-response round trip, not a long-lived
// stream like a worker session, so this is short: there is nothing here
// shaped like the gateway's drain-for-idle-workers problem.
const ShutdownTimeout = 5 * time.Second

// maxBody bounds the payload a single request may carry. Signals are meant to
// carry a small assertion — an approval, a callback's reference id — not an
// upload; a caller that needs to hand a run something larger should have the
// run fetch it through a tool instead.
const maxBody = 1 << 20 // 1 MiB

// Engine is what this package needs from the runtime.
//
// A subset of *engine.Engine's methods, named here so the package can be
// tested against a fake rather than a live store.
type Engine interface {
	// Signal delivers sig and gives the run an immediate chance to act on it.
	Signal(ctx context.Context, sig core.Signal) error

	// History reads a run's events, used only to report back whether the run
	// is (still) waiting on the signal just delivered.
	History(ctx context.Context, id core.RunID) ([]core.Event, error)
}

// Server owns the listener and the HTTP server serving one Engine.
type Server struct {
	eng  Engine
	http *http.Server
	ln   net.Listener
	log  *slog.Logger
}

// Listen binds the address and prepares to serve.
//
// Binding happens here rather than inside Run, for the same reason
// gateway.Listen does it here: a port already in use is a startup error the
// operator sees immediately, not a goroutine failing quietly after the
// process claims to be ready.
func Listen(addr string, eng Engine, log *slog.Logger) (*Server, error) {
	if eng == nil {
		return nil, errors.New("signalhttp: an Engine is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if addr == "" {
		addr = DefaultAddr
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("signalhttp: listen on %s: %w", addr, err)
	}

	h := &handler{eng: eng, log: log, ids: idgen.Random{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runs/{run}/signals/{name}", h.deliver)

	return &Server{
		eng:  eng,
		http: &http.Server{Handler: mux},
		ln:   ln,
		log:  log,
	}, nil
}

// Addr reports the bound address, which is how a test discovers the port
// when it asked for :0.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Run serves until ctx is cancelled, then shuts down within ShutdownTimeout.
func (s *Server) Run(ctx context.Context) error {
	errs := make(chan error, 1)
	go func() {
		s.log.Info("signal HTTP server listening", "addr", s.Addr())
		errs <- s.http.Serve(s.ln)
	}()

	select {
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("signalhttp: serve: %w", err)

	case <-ctx.Done():
		s.log.Info("signal HTTP server stopping", "addr", s.Addr())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
		defer cancel()
		if err := s.http.Shutdown(shutdownCtx); err != nil {
			// A delivery mid-flight when the timeout hit. The signal it carried
			// is either already recorded -- in which case the sender's retry
			// under the same Idempotency-Key is a no-op -- or it never reached
			// RecordSignal, in which case the same retry is the recovery. Either
			// way closing the connection costs the sender a retry, not a lost
			// signal.
			s.log.Warn("signal HTTP server: forced close after shutdown timeout", "error", err)
			_ = s.http.Close()
		}
		<-errs
		return nil
	}
}

// Close stops the server immediately. For tests and for a failed startup,
// where there is nothing in flight worth waiting for.
func (s *Server) Close() error { return s.http.Close() }

// handler serves one route: deliver a signal to a run.
type handler struct {
	eng Engine
	log *slog.Logger
	ids core.IDGen
}

type deliverResponse struct {
	RunID    string `json:"run_id"`
	SignalID string `json:"signal_id"`
	Name     string `json:"name"`

	// WaitingAtStep is set when the run is (still) parked on this exact
	// signal after delivery. Empty does not mean the delivery failed -- a
	// signal stored ahead of the run reaching its wait is stored just the
	// same, and is answered the same way a late one is.
	WaitingAtStep string `json:"waiting_at_step,omitempty"`
}

// deliver handles POST /v1/runs/{run}/signals/{name}.
//
// The idempotency key comes from a header, matching the vocabulary
// core.ToolCall already uses for the same concept: an Idempotency-Key a
// sender presents so that its own retry is a no-op rather than a second
// approval. One is generated when the caller omits it, which is the right
// default for an ad hoc call and the wrong one for anything automated --
// exactly the trade-off `veya signal --id` documents on the CLI side.
func (h *handler) deliver(w http.ResponseWriter, r *http.Request) {
	runID := core.RunID(r.PathValue("run"))
	name := r.PathValue("name")
	if runID == "" || name == "" {
		writeError(w, http.StatusBadRequest, "a signal needs a run id and a name")
		return
	}

	id := core.SignalID(r.Header.Get("Idempotency-Key"))
	if id == "" {
		id = core.SignalID(h.ids.NewEffectID())
	}

	var payload json.RawMessage
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		if err != nil {
			writeError(w, http.StatusBadRequest, "could not read the request body")
			return
		}
		if len(body) > maxBody {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", maxBody))
			return
		}
		if len(body) > 0 {
			if !json.Valid(body) {
				writeError(w, http.StatusBadRequest, "the request body is not valid JSON")
				return
			}
			payload = body
		}
	}

	sig := core.Signal{RunID: runID, ID: id, Name: name, Payload: payload}
	err := h.eng.Signal(r.Context(), sig)
	switch {
	case errors.Is(err, core.ErrNotFound):
		writeError(w, http.StatusNotFound, fmt.Sprintf("run %s does not exist", runID))
		return
	case errors.Is(err, core.ErrInvalidTransition):
		// The run has already finished. Refused rather than stored: nothing
		// will ever read a signal for a run that cannot advance, and accepting
		// it would tell the sender its callback landed somewhere that mattered.
		writeError(w, http.StatusConflict, fmt.Sprintf("run %s has already finished", runID))
		return
	case err != nil:
		h.log.Error("signal delivery failed", "run_id", runID, "signal_id", id, "name", name, "error", err)
		writeError(w, http.StatusInternalServerError, "the signal could not be recorded")
		return
	}

	resp := deliverResponse{RunID: string(runID), SignalID: string(id), Name: name}
	if history, herr := h.eng.History(r.Context(), runID); herr == nil {
		if wait, waiting := core.PendingWait(history); waiting && wait.Kind == core.WaitSignal && wait.Signal == name {
			resp.WaitingAtStep = string(wait.StepID)
		}
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: msg})
}
