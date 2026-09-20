package core_test

import (
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

func TestNoWaitInAnOrdinaryHistory(t *testing.T) {
	history := []core.Event{
		event(t, 1, core.EventRunStarted, "", core.RunStartedData{AgentName: "a"}),
		event(t, 2, core.EventTaskCreated, core.Step(1), core.TaskCreatedData{TaskID: "t1"}),
	}

	if _, waiting := core.PendingWait(history); waiting {
		t.Fatal("a history with no timer reports a pending wait")
	}
}

func TestAWaitIsOpenUntilItsOwnStepCloses(t *testing.T) {
	wake := time.Date(2026, time.September, 21, 9, 0, 0, 0, time.UTC)

	set := []core.Event{
		event(t, 1, core.EventRunStarted, "", core.RunStartedData{AgentName: "a"}),
		event(t, 2, core.EventTimerSet, core.Step(2), core.TimerSetData{WakeAt: wake}),
	}

	got, waiting := core.PendingWait(set)
	if !waiting {
		t.Fatal("a TIMER_SET with no TIMER_FIRED is not reported as a pending wait")
	}
	if got.StepID != core.Step(2) || !got.Until.Equal(wake) {
		t.Fatalf("got %+v, want step S2 waking at %s", got, wake)
	}

	fired := append(set,
		event(t, 3, core.EventTimerFired, core.Step(2), core.TimerFiredData{WakeAt: wake}))
	if _, waiting := core.PendingWait(fired); waiting {
		t.Fatal("a fired timer is still reported as pending")
	}
}

// TestAFireForAnotherStepDoesNotCloseThisWait. The pairing is by step, not by
// arrival: closing on any TIMER_FIRED would let a stale event from an earlier
// sleep release a run that is in the middle of a later one.
func TestAFireForAnotherStepDoesNotCloseThisWait(t *testing.T) {
	first := time.Date(2026, time.September, 21, 9, 0, 0, 0, time.UTC)
	second := first.Add(24 * time.Hour)

	history := []core.Event{
		event(t, 1, core.EventTimerSet, core.Step(1), core.TimerSetData{WakeAt: first}),
		event(t, 2, core.EventTimerFired, core.Step(1), core.TimerFiredData{WakeAt: first}),
		event(t, 3, core.EventTimerSet, core.Step(4), core.TimerSetData{WakeAt: second}),
		event(t, 4, core.EventTimerFired, core.Step(1), core.TimerFiredData{WakeAt: first}),
	}

	got, waiting := core.PendingWait(history)
	if !waiting {
		t.Fatal("a repeat of an older step's TIMER_FIRED closed the current wait")
	}
	if got.StepID != core.Step(4) {
		t.Fatalf("pending wait is at %s, want S4", got.StepID)
	}
}

// TestTheLastWaitWins. A run that sleeps twice has two TIMER_SET events, and
// the open one is the later; reporting the first would park the run against an
// instant that has long passed and resume it immediately.
func TestTheLastWaitWins(t *testing.T) {
	first := time.Date(2026, time.September, 21, 9, 0, 0, 0, time.UTC)
	second := first.Add(72 * time.Hour)

	history := []core.Event{
		event(t, 1, core.EventTimerSet, core.Step(1), core.TimerSetData{WakeAt: first}),
		event(t, 2, core.EventTimerFired, core.Step(1), core.TimerFiredData{WakeAt: first}),
		event(t, 3, core.EventTaskCreated, core.Step(2), core.TaskCreatedData{TaskID: "t"}),
		event(t, 4, core.EventTimerSet, core.Step(3), core.TimerSetData{WakeAt: second}),
	}

	got, waiting := core.PendingWait(history)
	if !waiting {
		t.Fatal("the second sleep is not reported as pending")
	}
	if !got.Until.Equal(second) {
		t.Fatalf("Until = %s, want the later wake %s", got.Until, second)
	}
}

func TestAWaitDueNowHasElapsed(t *testing.T) {
	now := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)

	if !(core.Wait{Until: now}).Elapsed(now) {
		t.Fatal("a wait due at exactly now has not elapsed; the boundary must be inclusive")
	}
	if !(core.Wait{Until: now.Add(-time.Hour)}).Elapsed(now) {
		t.Fatal("an overdue wait has not elapsed; a past wake-up is ready, not late")
	}
	if (core.Wait{Until: now.Add(time.Nanosecond)}).Elapsed(now) {
		t.Fatal("a wait due a nanosecond from now has already elapsed")
	}
	if (core.Wait{Until: core.Indefinite}).Elapsed(now) {
		t.Fatal("an indefinite wait elapsed on its own")
	}
}

func event(t *testing.T, seq int64, typ core.EventType, step core.StepID, data any) core.Event {
	t.Helper()
	e, err := core.NewEvent("run-wait", seq, typ, step, data)
	if err != nil {
		t.Fatalf("NewEvent(%s): %v", typ, err)
	}
	return e
}
