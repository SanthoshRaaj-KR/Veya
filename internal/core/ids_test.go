package core_test

import (
	"fmt"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// TestChildrenAreNamedByInvocationOrder is the property the whole fan-out
// design rests on.
//
// The idempotency key is derived from the step ID. Numbering children by
// completion order would give the same logical call a different key every time
// the race went differently, so a replayed fan-out would reserve fresh ledger
// rows and perform every call again — the exact duplicate the ledger exists to
// prevent, reintroduced by a naming choice.
func TestChildrenAreNamedByInvocationOrder(t *testing.T) {
	parent := core.Step(3)

	got := parent.Children(4)
	want := []core.StepID{"S3.0", "S3.1", "S3.2", "S3.3"}

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Children(4) = %v, want %v", got, want)
	}

	// The same call, made again in the same order, names the same steps. That
	// is what replay depends on, and it is true here only because nothing in
	// this function observes anything but the index.
	if fmt.Sprint(parent.Children(4)) != fmt.Sprint(got) {
		t.Fatal("Children is not a pure function of its arguments")
	}
}

func TestParentIsTheInverseOfChild(t *testing.T) {
	for _, parent := range []core.StepID{"S1", "S12", core.Step(7)} {
		for i := 0; i < 3; i++ {
			child := parent.Child(i)

			back, ok := child.Parent()
			if !ok {
				t.Fatalf("%s does not read as a child", child)
			}
			if back != parent {
				t.Fatalf("%s.Parent() = %s, want %s", child, back, parent)
			}
			if got := child.ChildIndex(); got != i {
				t.Fatalf("%s.ChildIndex() = %d, want %d", child, got, i)
			}
		}
	}
}

// TestOnlyANumericSuffixIsAChild.
//
// Grouping a fan-out means grouping children by their parent, so a step that
// merely contains a dot must not be swept into a join it has nothing to do
// with. The check is on the format rather than on intent, because history is
// read back by code that was not there when the step was named.
func TestOnlyANumericSuffixIsAChild(t *testing.T) {
	notChildren := []core.StepID{
		"S1",        // a plain step
		"",          // nothing
		".",         // no parent, no index
		".0",        // no parent
		"S1.",       // no index
		"S1.retry",  // a name, not a position
		"S1.0extra", // not a number
	}

	for _, s := range notChildren {
		if parent, ok := s.Parent(); ok {
			t.Errorf("%q read as a child of %q", s, parent)
		}
		if got := s.ChildIndex(); got != -1 {
			t.Errorf("%q.ChildIndex() = %d, want -1", s, got)
		}
	}
}

// TestNestedChildrenReadAsChildrenOfTheirImmediateParent. Nothing creates
// these yet — a child does not fan out again in this layer — but the format
// admits them, so the parse has to be unambiguous now rather than after
// somebody's history contains one.
func TestNestedChildrenReadAsChildrenOfTheirImmediateParent(t *testing.T) {
	nested := core.Step(1).Child(2).Child(3)
	if nested != "S1.2.3" {
		t.Fatalf("nested child = %s, want S1.2.3", nested)
	}

	parent, ok := nested.Parent()
	if !ok || parent != "S1.2" {
		t.Fatalf("%s.Parent() = %s (ok %v), want S1.2", nested, parent, ok)
	}
	if got := nested.ChildIndex(); got != 3 {
		t.Fatalf("%s.ChildIndex() = %d, want 3", nested, got)
	}
}
