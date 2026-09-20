package core_test

import (
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// fanOut builds a FanOut with n children, marking the ones named in settled
// with the result given.
func fanOut(join core.JoinPolicy, n int, outcomes map[int]bool) core.FanOut {
	f := core.FanOut{StepID: core.Step(1), Join: join}
	for i := 0; i < n; i++ {
		c := core.ChildOutcome{StepID: core.Step(1).Child(i), Index: i, Tool: "check"}
		if ok, settled := outcomes[i]; settled {
			c.Settled = true
			c.Succeeded = ok
		}
		f.Children = append(f.Children, c)
	}
	return f
}

func TestJoinAllWaitsForEveryChild(t *testing.T) {
	all := core.JoinPolicy{Kind: core.JoinAll}

	if fanOut(all, 3, map[int]bool{0: true, 1: true}).Satisfied() {
		t.Fatal("ALL was satisfied with one child still running")
	}
	// Including the ones that failed. ALL is about settling, not succeeding:
	// the body is handed every outcome and decides what they mean.
	if !fanOut(all, 3, map[int]bool{0: true, 1: false, 2: false}).Satisfied() {
		t.Fatal("ALL was not satisfied when every child had settled, because two failed")
	}
}

// TestJoinAnyHasTwoExits. One success ends it, and so does every child
// failing. A join satisfiable only by success hangs forever on a bad day.
func TestJoinAnyHasTwoExits(t *testing.T) {
	any := core.JoinPolicy{Kind: core.JoinAny}

	if fanOut(any, 3, nil).Satisfied() {
		t.Fatal("ANY was satisfied with nothing settled")
	}
	if !fanOut(any, 3, map[int]bool{1: true}).Satisfied() {
		t.Fatal("ANY was not satisfied by one success")
	}
	if fanOut(any, 3, map[int]bool{0: false, 1: false}).Satisfied() {
		t.Fatal("ANY gave up while a child could still succeed")
	}
	if !fanOut(any, 3, map[int]bool{0: false, 1: false, 2: false}).Satisfied() {
		t.Fatal("ANY hung after every child failed; there is no success coming")
	}
}

// TestJoinQuorumStopsWhenItBecomesImpossible is the exit that is easy to miss.
//
// QUORUM(3) over five children with three failures can never reach three
// successes. Waiting for the remaining two to settle would keep the run alive
// for an answer that is already decided, and a run waiting for something that
// cannot happen is the failure this layer exists to remove.
func TestJoinQuorumStopsWhenItBecomesImpossible(t *testing.T) {
	quorum := core.JoinPolicy{Kind: core.JoinQuorum, Quorum: 3}

	if fanOut(quorum, 5, map[int]bool{0: true, 1: true}).Satisfied() {
		t.Fatal("QUORUM(3) was satisfied by two successes")
	}
	if !fanOut(quorum, 5, map[int]bool{0: true, 1: true, 2: true}).Satisfied() {
		t.Fatal("QUORUM(3) was not satisfied by three successes")
	}
	if !fanOut(quorum, 5, map[int]bool{0: false, 1: false, 2: false}).Satisfied() {
		t.Fatal("QUORUM(3) over 5 with 3 failures waited on; it can no longer be met")
	}
	// One more failure than that still ends it, and one fewer does not.
	if fanOut(quorum, 5, map[int]bool{0: false, 1: false}).Satisfied() {
		t.Fatal("QUORUM(3) over 5 with 2 failures gave up; three successes are still possible")
	}
}

// TestARetryableFailureIsNotSettled.
//
// A failed attempt with retries left may still succeed. Counting it as a
// settled failure would have a quorum give up on a child that was about to
// work, and an ALL join report a failure the run never actually had.
func TestARetryableFailureIsNotSettled(t *testing.T) {
	history := []core.Event{
		event(t, 1, core.EventFanOutStarted, core.Step(1), core.FanOutStartedData{
			Calls: []string{"check", "check"}, Join: "ALL",
		}),
		event(t, 2, core.EventTaskFailed, core.Step(1).Child(0), core.TaskFailedData{
			Error: "timeout", Attempt: 1, Final: false,
		}),
		event(t, 3, core.EventTaskCompleted, core.Step(1).Child(1), core.TaskCompletedData{}),
	}

	pending, waiting := core.PendingFanOut(history)
	if !waiting {
		t.Fatal("the fan-out reads as joined while a child is mid-retry")
	}
	if _, failed, settled := pending.Counts(); failed != 0 || settled != 1 {
		t.Fatalf("counts = failed %d, settled %d; a retryable failure is neither", failed, settled)
	}

	// The final attempt settles it.
	history = append(history, event(t, 4, core.EventTaskFailed, core.Step(1).Child(0),
		core.TaskFailedData{Error: "timeout", Attempt: 3, Final: true}))
	if _, waiting = core.PendingFanOut(history); waiting {
		t.Fatal("ALL is still waiting after every child settled")
	}
}

func TestPendingFanOutReadsChildrenInInvocationOrder(t *testing.T) {
	history := []core.Event{
		event(t, 1, core.EventFanOutStarted, core.Step(2), core.FanOutStartedData{
			Calls: []string{"stock", "price", "tax"}, Join: "QUORUM(2)",
		}),
		// Completed out of order on purpose: the third child first.
		event(t, 2, core.EventTaskCompleted, core.Step(2).Child(2), core.TaskCompletedData{}),
	}

	pending, waiting := core.PendingFanOut(history)
	if !waiting {
		t.Fatal("QUORUM(2) reads as satisfied by one success")
	}
	if pending.Join.Kind != core.JoinQuorum || pending.Join.Quorum != 2 {
		t.Fatalf("join = %s, want QUORUM(2); the policy did not survive history", pending.Join)
	}

	want := []string{"stock", "price", "tax"}
	for i, tool := range want {
		if pending.Children[i].Tool != tool {
			t.Fatalf("child %d is %q, want %q; children are ordered by invocation, "+
				"never by completion", i, pending.Children[i].Tool, tool)
		}
		if pending.Children[i].StepID != core.Step(2).Child(i) {
			t.Fatalf("child %d is step %s, want %s", i,
				pending.Children[i].StepID, core.Step(2).Child(i))
		}
	}
	if !pending.Children[2].Succeeded {
		t.Fatal("the child that completed first was not recorded against its own position")
	}
}

// TestAnUnreadablePolicyAsksTheBody. A fan-out written by a build that knew a
// policy this one does not must hand the decision back rather than park the
// run on a rule nobody can evaluate. Silence is the worse failure.
func TestAnUnreadablePolicyAsksTheBody(t *testing.T) {
	history := []core.Event{
		event(t, 1, core.EventFanOutStarted, core.Step(1), core.FanOutStartedData{
			Calls: []string{"a", "b"}, Join: "MAJORITY_WEIGHTED",
		}),
	}

	if _, waiting := core.PendingFanOut(history); waiting {
		t.Fatal("a policy this build cannot evaluate blocked the run; it must ask the body")
	}
}

// TestASecondFanOutSupersedesTheFirst. A run that fans out twice has two
// FAN_OUT_STARTED events, and only the later one can still be waiting.
func TestASecondFanOutSupersedesTheFirst(t *testing.T) {
	history := []core.Event{
		event(t, 1, core.EventFanOutStarted, core.Step(1), core.FanOutStartedData{
			Calls: []string{"a"}, Join: "ALL",
		}),
		event(t, 2, core.EventTaskCompleted, core.Step(1).Child(0), core.TaskCompletedData{}),
		event(t, 3, core.EventFanOutStarted, core.Step(2), core.FanOutStartedData{
			Calls: []string{"b", "c"}, Join: "ALL",
		}),
		event(t, 4, core.EventTaskCompleted, core.Step(2).Child(0), core.TaskCompletedData{}),
	}

	pending, waiting := core.PendingFanOut(history)
	if !waiting {
		t.Fatal("the second fan-out reads as joined with one child outstanding")
	}
	if pending.StepID != core.Step(2) {
		t.Fatalf("pending fan-out is at %s, want S2", pending.StepID)
	}
	if len(pending.Children) != 2 {
		t.Fatalf("pending fan-out has %d children, want 2", len(pending.Children))
	}
}
