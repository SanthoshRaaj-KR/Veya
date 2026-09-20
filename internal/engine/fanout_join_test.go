package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// TestTenChildrenThreeFailuresOneDeterministicOrder is the layer's exit
// criterion for fan-out.
//
// Ten calls go out at once, three of them fail for good, and the seven that
// succeed do so in whatever order the workers happen to finish. What the body
// sees has to be the same every time: ten outcomes, in invocation order, with
// the failures in the positions they were invoked from — not the positions
// they finished in, because those are not reproducible and the next replay
// would disagree with this one.
func TestTenChildrenThreeFailuresOneDeterministicOrder(t *testing.T) {
	const children = 10
	failing := map[int]bool{2: true, 5: true, 6: true}

	h := newHarness(t, joiningAgent(children, core.JoinPolicy{Kind: core.JoinAll}))

	// The tool fails for three of the ten, permanently, and succeeds slowly
	// and unevenly for the rest so that completion order is not invocation
	// order. A test where they finish in order proves nothing.
	var mu sync.Mutex
	order := 0
	h.tools.Func("check", func(_ context.Context, call core.ToolCall) ([]byte, error) {
		var payload struct {
			I int `json:"i"`
		}
		if err := json.Unmarshal(call.Payload, &payload); err != nil {
			return nil, err
		}
		if failing[payload.I] {
			return nil, core.NotExecuted(fmt.Errorf("child %d is refusing", payload.I))
		}

		mu.Lock()
		order++
		finished := order
		mu.Unlock()
		return json.RawMessage(fmt.Sprintf(`{"i":%d,"finished":%d}`, payload.I, finished)), nil
	})
	h.startWorkers(t, 4)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED; ALL joins on settling, not succeeding",
			run.Status, run.LastError)
	}

	// The outcomes the body was handed, as it recorded them in the output.
	var got struct {
		Outcomes []string `json:"outcomes"`
	}
	if err := json.Unmarshal(run.Output, &got); err != nil {
		t.Fatalf("decode run output %s: %v", run.Output, err)
	}
	if len(got.Outcomes) != children {
		t.Fatalf("the body saw %d outcomes, want %d", len(got.Outcomes), children)
	}

	for i, outcome := range got.Outcomes {
		want := "ok"
		if failing[i] {
			want = "failed"
		}
		if outcome != want {
			t.Fatalf("outcome %d is %q, want %q; the join must report by invocation "+
				"order, not by the order things finished (%v)", i, outcome, want, got.Outcomes)
		}
	}

	// And completion order really was different, or this test would pass
	// trivially and stop meaning anything.
	assertCompletionOrderDiffered(t, h.history(t, runID), children, failing)
}

// TestAnyProceedsOnTheFirstSuccessAndLeavesSiblingsRunning.
//
// ANY finishes while siblings are still in flight. They are not cancelled —
// that is Layer 6, and stopping them is a different mechanism from ignoring
// them — so their effects land and are still recorded. A ledger with an orphan
// in it would be worse than a slow child.
func TestAnyProceedsOnTheFirstSuccessAndLeavesSiblingsRunning(t *testing.T) {
	const children = 3

	h := newHarness(t, joiningAgent(children, core.JoinPolicy{Kind: core.JoinAny}))

	// The first child returns at once; the other two block until the test
	// releases them, so the join is provably satisfied with siblings running.
	release := make(chan struct{})
	var first sync.Once
	firstDone := make(chan struct{})
	h.tools.Func("check", func(ctx context.Context, call core.ToolCall) ([]byte, error) {
		var payload struct {
			I int `json:"i"`
		}
		if err := json.Unmarshal(call.Payload, &payload); err != nil {
			return nil, err
		}
		if payload.I == 0 {
			first.Do(func() { close(firstDone) })
			return json.RawMessage(`{"i":0}`), nil
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.RawMessage(fmt.Sprintf(`{"i":%d}`, payload.I)), nil
	})
	h.startWorkers(t, 3)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	<-firstDone
	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED on the first success", run.Status, run.LastError)
	}

	// The run is finished with two children still working. Let them land.
	close(release)

	// Their completions are still recorded, after RUN_COMPLETED, which is not
	// tidy and is true -- and true beats tidy in an audit trail.
	h.awaitEventCount(t, runID, core.EventTaskCompleted, children)
	types := typesOf(h.history(t, runID))
	completedAt := -1
	for i, typ := range types {
		if typ == core.EventRunCompleted {
			completedAt = i
		}
	}
	if completedAt < 0 {
		t.Fatalf("no RUN_COMPLETED in %v", types)
	}
	late := 0
	for _, typ := range types[completedAt+1:] {
		if typ == core.EventTaskCompleted {
			late++
		}
	}
	if late == 0 {
		t.Fatalf("no child landed after the run completed; the test did not "+
			"reach the state it exists for (%v)", types)
	}
}

// joiningAgent fans out n calls under a policy, then completes with one label
// per child, in invocation order, once the join is satisfied.
//
// It reads the outcomes out of history itself rather than through
// core.PendingFanOut, so that the ordering claim is checked against the event
// log rather than against the same code that produces the answer.
func joiningAgent(n int, join core.JoinPolicy) deciderFunc {
	return func(_ core.Run, history []core.Event) (core.Decision, error) {
		parent := core.Step(1)
		outcomes := make([]string, n)
		settled := 0
		succeeded := 0

		for _, e := range history {
			idx := indexOfChild(parent, e.StepID, n)
			if idx < 0 {
				continue
			}
			switch e.Type {
			case core.EventTaskCompleted:
				if outcomes[idx] == "" {
					outcomes[idx] = "ok"
					settled++
					succeeded++
				}
			case core.EventTaskFailed:
				var data core.TaskFailedData
				if err := e.Decode(&data); err != nil || !data.Final {
					continue
				}
				if outcomes[idx] == "" {
					outcomes[idx] = "failed"
					settled++
				}
			}
		}

		done := false
		switch join.Kind {
		case core.JoinAll:
			done = settled == n
		case core.JoinAny:
			done = succeeded > 0 || settled == n
		case core.JoinQuorum:
			done = succeeded >= join.Quorum
		}
		if done {
			for i := range outcomes {
				if outcomes[i] == "" {
					outcomes[i] = "pending"
				}
			}
			out, err := json.Marshal(map[string]any{"outcomes": outcomes})
			if err != nil {
				return core.Decision{}, err
			}
			return core.Decision{Kind: core.DecideComplete, Output: out}, nil
		}

		calls := make([]core.Call, n)
		for i := range calls {
			calls[i] = core.Call{
				TaskType: "check",
				Payload:  json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			}
		}
		return core.Decision{
			Kind: core.DecideCallToolParallel, StepID: parent, Calls: calls, Join: join,
		}, nil
	}
}

// indexOfChild returns the position of step within parent's fan-out, or -1.
func indexOfChild(parent, step core.StepID, n int) int {
	for i := 0; i < n; i++ {
		if step == parent.Child(i) {
			return i
		}
	}
	return -1
}

// assertCompletionOrderDiffered fails if the children happened to finish in
// invocation order, which would make the ordering assertion above vacuous.
func assertCompletionOrderDiffered(t *testing.T, history []core.Event, n int, failing map[int]bool) {
	t.Helper()

	var finished []int
	for _, e := range history {
		if e.Type != core.EventTaskCompleted {
			continue
		}
		if idx := indexOfChild(core.Step(1), e.StepID, n); idx >= 0 {
			finished = append(finished, idx)
		}
	}

	var invocation []int
	for i := 0; i < n; i++ {
		if !failing[i] {
			invocation = append(invocation, i)
		}
	}
	if len(finished) != len(invocation) {
		t.Fatalf("%d children completed, want %d", len(finished), len(invocation))
	}

	sorted := append([]int(nil), finished...)
	sort.Ints(sorted)
	if fmt.Sprint(sorted) != fmt.Sprint(invocation) {
		t.Fatalf("the set of completed children is %v, want %v", sorted, invocation)
	}
	if fmt.Sprint(finished) == fmt.Sprint(invocation) {
		t.Logf("children finished in invocation order this run (%v); the ordering "+
			"assertion is weaker than intended on this scheduling", finished)
	}
}

// awaitEventCount polls until a run's history holds at least n events of a
// type, or the test times out.
func (h *harness) awaitEventCount(t *testing.T, id core.RunID, typ core.EventType, n int) {
	t.Helper()

	for i := 0; i < 500; i++ {
		if countEvents(h.history(t, id), typ) >= n {
			return
		}
		time.Sleep(4 * time.Millisecond)
	}
	t.Fatalf("run %s never reached %d %s events; history: %s",
		id, n, typ, strings.Join(typeNames(h.history(t, id)), " "))
}

func typeNames(history []core.Event) []string {
	out := make([]string, 0, len(history))
	for _, e := range history {
		out = append(out, string(e.Type))
	}
	return out
}
