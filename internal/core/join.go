package core

import (
	"encoding/json"
	"fmt"
)

// ChildOutcome is what became of one call in a fan-out.
//
// Settled is terminal in the task sense: the child will not be tried again.
// A failed attempt with retries left is not settled, because the next attempt
// may succeed and a join that counted it as failed would give up early.
type ChildOutcome struct {
	StepID StepID
	Index  int
	Tool   string

	Settled   bool
	Succeeded bool

	Result json.RawMessage
	Error  string
}

// FanOut is one parallel decision and everything history says about it.
type FanOut struct {
	StepID   StepID
	Join     JoinPolicy
	Children []ChildOutcome // in invocation order, always
}

// Counts returns how many children have succeeded, failed, and settled either
// way.
func (f FanOut) Counts() (succeeded, failed, settled int) {
	for _, c := range f.Children {
		if !c.Settled {
			continue
		}
		settled++
		if c.Succeeded {
			succeeded++
		} else {
			failed++
		}
	}
	return succeeded, failed, settled
}

// Satisfied reports whether the join has finished waiting.
//
// It says nothing about whether the run should be happy with the result.
// Whether three failures out of ten is a disaster or a Tuesday is a question
// about meaning, and the engine does not do meaning — every outcome goes back
// to the agent body, in invocation order, and the body decides.
//
// The second exit in each case is the one that gets forgotten. A join
// satisfiable only by success hangs forever on a bad day, and a run that hangs
// forever is the failure this whole layer exists to remove.
func (f FanOut) Satisfied() bool {
	succeeded, failed, settled := f.Counts()
	total := len(f.Children)

	switch f.Join.Kind {
	case JoinAll:
		return settled == total

	case JoinAny:
		// One success ends it — or every child failing, because then no
		// success is coming.
		return succeeded > 0 || failed == total

	case JoinQuorum:
		// Enough successes, or too few children left for enough to be
		// possible. total-failed is the best case still available.
		return succeeded >= f.Join.Quorum || total-failed < f.Join.Quorum

	default:
		// A policy this build cannot evaluate is treated as finished, so the
		// body is asked and can say what it wants. The alternative is a run
		// parked on a rule nobody can read, which is the worse of the two:
		// this one produces a decision, that one produces silence.
		return true
	}
}

// PendingFanOut returns the run's most recent fan-out whose join is not yet
// satisfied.
//
// Like PendingWait, it is a pure function over history, and for the same
// reason: the engine needs to know whether asking the decider can produce
// anything new, and a column recording it would be a second copy of a fact
// history already holds.
//
// It is only ever used to *skip* work. If it is wrong, the decider is asked a
// question it answers with the same fan-out it already issued, which commits
// nothing. That asymmetry is deliberate — the body remains the authority on
// what a join means, and this is an optimisation that cannot overrule it.
func PendingFanOut(history []Event) (FanOut, bool) {
	var (
		open  FanOut
		found bool
		index map[StepID]int
	)

	for _, e := range history {
		switch e.Type {
		case EventFanOutStarted:
			var data FanOutStartedData
			if err := e.Decode(&data); err != nil {
				// Unreadable: report nothing pending, so the body is asked.
				// Skipping the decider on a fan-out this build cannot parse
				// would park the run on a rule it cannot check.
				return FanOut{}, false
			}
			open = FanOut{StepID: e.StepID, Join: joinFrom(data.Join)}
			open.Children = make([]ChildOutcome, len(data.Calls))
			index = make(map[StepID]int, len(data.Calls))
			for i, tool := range data.Calls {
				child := e.StepID.Child(i)
				open.Children[i] = ChildOutcome{StepID: child, Index: i, Tool: tool}
				index[child] = i
			}
			found = true

		case EventTaskCompleted:
			i, ok := index[e.StepID]
			if !found || !ok {
				continue
			}
			var data TaskCompletedData
			_ = e.Decode(&data)
			open.Children[i].Settled = true
			open.Children[i].Succeeded = true
			open.Children[i].Result = data.Result

		case EventTaskFailed:
			i, ok := index[e.StepID]
			if !found || !ok {
				continue
			}
			var data TaskFailedData
			if err := e.Decode(&data); err != nil {
				continue
			}
			open.Children[i].Error = data.Error
			// Final is the difference between "this child is done and it
			// failed" and "this attempt failed and another is coming". A join
			// that counted a retryable failure as settled would give up on a
			// child that was about to succeed.
			if data.Final {
				open.Children[i].Settled = true
				open.Children[i].Succeeded = false
			}
		}
	}

	if !found || open.Satisfied() {
		return FanOut{}, false
	}
	return open, true
}

// joinFrom parses the policy as FanOutStartedData records it.
//
// An unrecognised string becomes a zero JoinPolicy, whose Satisfied returns
// true — so a fan-out written by a build that knew a policy this one does not
// is handed back to the body rather than blocking the run.
func joinFrom(s string) JoinPolicy {
	switch {
	case s == string(JoinAll):
		return JoinPolicy{Kind: JoinAll}
	case s == string(JoinAny):
		return JoinPolicy{Kind: JoinAny}
	default:
		var n int
		if _, err := fmt.Sscanf(s, "QUORUM(%d)", &n); err == nil && n > 0 {
			return JoinPolicy{Kind: JoinQuorum, Quorum: n}
		}
		return JoinPolicy{}
	}
}
