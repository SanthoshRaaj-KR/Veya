package core_test

import (
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

func TestAQuorumLargerThanTheFanOutIsRefused(t *testing.T) {
	// The failure this prevents is silent: an unsatisfiable quorum parks the
	// run forever with nothing in the logs to suggest why. Catching it where
	// the policy is stated names the agent that wrote it.
	policy := core.JoinPolicy{Kind: core.JoinQuorum, Quorum: 4}

	if err := policy.Valid(3); err == nil {
		t.Fatal("QUORUM(4) over 3 calls was accepted; it can never be satisfied")
	}
	if err := policy.Valid(4); err != nil {
		t.Fatalf("QUORUM(4) over 4 calls: %v", err)
	}
}

func TestJoinPolicyValidity(t *testing.T) {
	cases := map[string]struct {
		policy core.JoinPolicy
		n      int
		ok     bool
	}{
		"all":            {core.JoinPolicy{Kind: core.JoinAll}, 3, true},
		"any":            {core.JoinPolicy{Kind: core.JoinAny}, 3, true},
		"quorum of one":  {core.JoinPolicy{Kind: core.JoinQuorum, Quorum: 1}, 3, true},
		"quorum of zero": {core.JoinPolicy{Kind: core.JoinQuorum, Quorum: 0}, 3, false},
		"negative":       {core.JoinPolicy{Kind: core.JoinQuorum, Quorum: -1}, 3, false},
		"unknown kind":   {core.JoinPolicy{Kind: "MOST"}, 3, false},
		"empty kind":     {core.JoinPolicy{}, 3, false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.policy.Valid(tc.n)
			if tc.ok && err != nil {
				t.Fatalf("%s over %d calls: %v", tc.policy, tc.n, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("%s over %d calls was accepted", tc.policy, tc.n)
			}
		})
	}
}

func TestIndefiniteIsBeyondAnyRealSchedule(t *testing.T) {
	// The sentinel only works if nothing a caller could plausibly schedule
	// reads as indefinite, and if the value itself always does.
	if !core.IsIndefinite(core.Indefinite) {
		t.Fatal("core.Indefinite does not read as indefinite")
	}

	far := time.Now().AddDate(500, 0, 0)
	if core.IsIndefinite(far) {
		t.Fatalf("a wake-up %s reads as indefinite; the sentinel is too close", far)
	}
	if core.IsIndefinite(time.Time{}) {
		t.Fatal("the zero time reads as indefinite; unset must not mean forever")
	}
}

func TestOnlySuspendingKindsParkARun(t *testing.T) {
	// A run waits on time or on tasks and never both, which holds only
	// because the kinds that park cannot also carry calls.
	parks := map[core.DecisionKind]bool{
		core.DecideSleep:            true,
		core.DecideWaitForSignal:    true,
		core.DecideCallTool:         false,
		core.DecideCallToolParallel: false,
		core.DecideComplete:         false,
		core.DecideFail:             false,
	}

	for kind, want := range parks {
		if got := kind.IsSuspension(); got != want {
			t.Errorf("%s.IsSuspension() = %v, want %v", kind, got, want)
		}
	}
}
