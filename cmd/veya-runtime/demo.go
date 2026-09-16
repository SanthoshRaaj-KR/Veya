package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SanthoshRaaj-KR/Veya/internal/decider"
	"github.com/SanthoshRaaj-KR/Veya/internal/tool"
)

// The built-in demo agent: the meeting assistant from docs/architecture-primer.md.
//
// Three steps — look the meeting up, summarize it, send the summary — which is
// enough to exercise the whole Layer 1 path without a language model or a real
// external service in the way.
//
// Layer 2 is where send_summary becomes interesting: it is the step with an
// external consequence, so it is the one that gets an effect row, an
// idempotency key, and a reconciliation path. Today it only pretends.

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

	r.Func("fetch_meeting", func(_ context.Context, payload []byte) ([]byte, error) {
		var in struct {
			MeetingID string `json:"meeting_id"`
		}
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, fmt.Errorf("fetch_meeting: bad payload: %w", err)
		}
		return json.Marshal(map[string]any{
			"meeting_id":   in.MeetingID,
			"title":        "Quarterly planning",
			"participants": []string{"ana@example.com", "bo@example.com"},
			"notes":        "Agreed to ship Layer 1. Revisit dispatch benchmarks next month.",
		})
	})

	r.Func("summarize", func(_ context.Context, _ []byte) ([]byte, error) {
		// A real agent would call a model here. From Layer 2 that call gets an
		// effect row of its own, because a model call is billed and cannot be
		// replayed by the provider — treating it as a free read is how a crash
		// mid-completion silently bills twice.
		return json.Marshal(map[string]any{
			"summary": "Layer 1 ships. Dispatch benchmarks deferred to next month.",
		})
	})

	r.Func("send_summary", func(_ context.Context, payload []byte) ([]byte, error) {
		var in struct {
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, fmt.Errorf("send_summary: bad payload: %w", err)
		}
		// Layer 2 replaces this with a real effect: reserve the row, commit it
		// before the call, and resolve UNKNOWN by reconciliation rather than
		// by guessing.
		return json.Marshal(map[string]any{
			"channel":   in.Channel,
			"delivered": true,
			"reference": "msg_" + strings.ToUpper(in.Channel) + "_98374",
		})
	})

	return r
}
