package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/decider"
	"github.com/SanthoshRaaj-KR/Veya/internal/tool"
)

// The built-in demo agent: the meeting assistant from docs/architecture-primer.md.
//
// Three steps — look the meeting up, summarize it, send the summary — chosen
// because between them they exercise all three interesting effect classes:
//
//	fetch_meeting  NONE               a pure read; no ledger row at all
//	summarize      IDEMPOTENT_BY_KEY  a model call, which is an effect
//	send_summary   QUERYABLE          can be asked what it did afterwards
//
// The providers are fakes, but they honour their declared contracts, which is
// the part that matters: a tool that claims to be QUERYABLE and cannot
// actually answer is a lie the runtime would only discover during an outage.

const (
	demoAgentName    = "meeting_assistant"
	demoAgentVersion = "v1"
)

func demoDecider() *decider.Static {
	return decider.NewStatic(
		decider.Step{Tool: "fetch_meeting", Payload: json.RawMessage(`{"meeting_id":"M-1042"}`)},
		decider.Step{Tool: "summarize", Payload: json.RawMessage(`{"style":"brief"}`)},
		decider.Step{Tool: "send_summary", Payload: json.RawMessage(`{"channel":"email"}`)},
	)
}

func demoTools() *tool.Registry {
	r := tool.New()
	mailbox := newFakeMailbox()

	// A pure read. It changes nothing, so running it twice costs latency and
	// nothing else, and it bypasses the ledger entirely.
	r.Func("fetch_meeting", func(_ context.Context, payload []byte) ([]byte, error) {
		var in struct {
			MeetingID string `json:"meeting_id"`
		}
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, core.NotExecuted(fmt.Errorf("fetch_meeting: bad payload: %w", err))
		}
		return json.Marshal(map[string]any{
			"meeting_id":   in.MeetingID,
			"title":        "Quarterly planning",
			"participants": []string{"ana@example.com", "bo@example.com"},
			"notes":        "Agreed to ship Layer 2. Dispatch benchmarks next.",
		})
	})

	// A model call, classified as an effect rather than a read.
	//
	// It looks like a harmless question and is not: it is billed, it is
	// non-deterministic, and the provider cannot replay it. Treating it as a
	// read means every crash mid-completion quietly pays for it again, with no
	// record that it ever happened.
	r.Effectful("summarize", core.ClassIdempotentByKey, 24*time.Hour,
		func(context.Context, []byte) ([]byte, error) {
			return json.Marshal(map[string]any{
				"summary":   "Layer 2 ships. Dispatch benchmarks next.",
				"reference": "cmpl_7731",
			})
		}, nil)

	// The step with a real external consequence, and the one Layer 2 exists
	// for. Declared QUERYABLE because the fake provider can be asked whether
	// it already sent a given key, so an ambiguous outcome is settled by
	// asking rather than by sending a second email.
	r.Effectful("send_summary", core.ClassQueryable, 24*time.Hour,
		func(_ context.Context, payload []byte) ([]byte, error) {
			var in struct {
				Channel string `json:"channel"`
			}
			if err := json.Unmarshal(payload, &in); err != nil {
				return nil, core.NotExecuted(fmt.Errorf("send_summary: bad payload: %w", err))
			}
			return mailbox.send(in.Channel)
		},
		core.ReconcilerFunc(func(_ context.Context, e core.Effect) (core.Resolution, error) {
			return mailbox.lookup(e.Key), nil
		}))

	return r
}

// fakeMailbox stands in for a provider that records what it sent and can be
// asked about it afterwards. It is the smallest thing that honestly implements
// the QUERYABLE contract, including the part that matters most: it records the
// send under the caller's idempotency key, so a later lookup can find it.
type fakeMailbox struct {
	mu   sync.Mutex
	sent map[core.IdempotencyKey]string
	seq  int
}

func newFakeMailbox() *fakeMailbox {
	return &fakeMailbox{sent: make(map[core.IdempotencyKey]string)}
}

func (m *fakeMailbox) send(channel string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	ref := fmt.Sprintf("msg_%s_%d", strings.ToUpper(channel), 98374+m.seq)
	return json.Marshal(map[string]any{
		"channel":   channel,
		"delivered": true,
		"reference": ref,
	})
}

func (m *fakeMailbox) lookup(key core.IdempotencyKey) core.Resolution {
	m.mu.Lock()
	defer m.mu.Unlock()

	ref, ok := m.sent[key]
	if !ok {
		return core.Resolution{
			Kind:   core.ResolvedNotExecuted,
			Detail: "no message recorded for this key",
		}
	}
	return core.Resolution{
		Kind:        core.ResolvedCommitted,
		ExternalRef: ref,
		Detail:      "found by idempotency key",
	}
}
