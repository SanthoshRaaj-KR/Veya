package signalhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/signalhttp"
)

// fakeEngine is the Engine interface, driven entirely by the test: no store,
// no decider, just what the handler actually calls and what it does with the
// answer. That is the point of naming the interface in the package doc.
type fakeEngine struct {
	err     error
	history []core.Event
	got     core.Signal
}

func (f *fakeEngine) Signal(_ context.Context, sig core.Signal) error {
	f.got = sig
	return f.err
}

func (f *fakeEngine) History(context.Context, core.RunID) ([]core.Event, error) {
	return f.history, nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newServer(t *testing.T, eng *fakeEngine) (*signalhttp.Server, string) {
	t.Helper()
	srv, err := signalhttp.Listen("127.0.0.1:0", eng, quiet())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	return srv, "http://" + srv.Addr()
}

func post(t *testing.T, url string, headers map[string]string, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func decode[T any](t *testing.T, r *http.Response) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return v
}

type deliverResponse struct {
	RunID         string `json:"run_id"`
	SignalID      string `json:"signal_id"`
	Name          string `json:"name"`
	WaitingAtStep string `json:"waiting_at_step,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func TestDeliverSucceedsAndReportsWhereTheRunIsWaiting(t *testing.T) {
	wait, err := core.NewEvent("run-1", 1, core.EventSignalWaitStarted, core.Step(3),
		core.SignalWaitStartedData{Name: "approval"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	eng := &fakeEngine{history: []core.Event{wait}}
	_, base := newServer(t, eng)

	resp := post(t, base+"/v1/runs/run-1/signals/approval", nil, `{"by":"ops"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	got := decode[deliverResponse](t, resp)
	if got.RunID != "run-1" || got.Name != "approval" {
		t.Fatalf("response = %+v", got)
	}
	if got.WaitingAtStep != "S3" {
		t.Fatalf("waiting_at_step = %q, want S3", got.WaitingAtStep)
	}
	if got.SignalID == "" {
		t.Fatal("no signal_id was generated for a request with no Idempotency-Key")
	}
	if eng.got.Payload == nil || string(eng.got.Payload) != `{"by":"ops"}` {
		t.Fatalf("Engine.Signal was called with payload %s, want the request body verbatim", eng.got.Payload)
	}
}

func TestIdempotencyKeyHeaderBecomesTheSignalID(t *testing.T) {
	eng := &fakeEngine{}
	_, base := newServer(t, eng)

	resp := post(t, base+"/v1/runs/run-1/signals/approval",
		map[string]string{"Idempotency-Key": "sender-retry-7"}, "")
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if eng.got.ID != "sender-retry-7" {
		t.Fatalf("signal id = %q, want the header's value", eng.got.ID)
	}
}

func TestUnknownRunIsNotFound(t *testing.T) {
	eng := &fakeEngine{err: fmt.Errorf("wrap: %w", core.ErrNotFound)}
	_, base := newServer(t, eng)

	resp := post(t, base+"/v1/runs/ghost/signals/approval", nil, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestAFinishedRunIsConflict(t *testing.T) {
	eng := &fakeEngine{err: fmt.Errorf("wrap: %w", core.ErrInvalidTransition)}
	_, base := newServer(t, eng)

	resp := post(t, base+"/v1/runs/run-1/signals/approval", nil, "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
}

func TestAnUnexpectedEngineErrorIsFiveHundred(t *testing.T) {
	eng := &fakeEngine{err: errors.New("store unreachable")}
	_, base := newServer(t, eng)

	resp := post(t, base+"/v1/runs/run-1/signals/approval", nil, "")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

func TestAnInvalidBodyIsRejected(t *testing.T) {
	eng := &fakeEngine{}
	_, base := newServer(t, eng)

	resp := post(t, base+"/v1/runs/run-1/signals/approval", nil, `{not json`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	got := decode[errorResponse](t, resp)
	if got.Error == "" {
		t.Fatal("a 400 must say why")
	}
}

func TestAnOversizedBodyIsRejected(t *testing.T) {
	eng := &fakeEngine{}
	_, base := newServer(t, eng)

	huge := bytes.Repeat([]byte("a"), (1<<20)+1)
	resp := post(t, base+"/v1/runs/run-1/signals/approval", nil, `{"x":"`+string(huge)+`"}`)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}
