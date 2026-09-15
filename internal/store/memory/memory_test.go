package memory_test

import (
	"testing"
	"time"

	"github.com/santhoshraajkr/veya/internal/clock"
	"github.com/santhoshraajkr/veya/internal/core"
	"github.com/santhoshraajkr/veya/internal/core/storetest"
	"github.com/santhoshraajkr/veya/internal/store/memory"
)

func TestStoreContract(t *testing.T) {
	storetest.RunStoreSuite(t, func(t *testing.T) core.Store {
		// A virtual clock that advances a tick per read, so that records
		// created in sequence have distinct, ordered timestamps without the
		// test depending on wall-clock resolution.
		return memory.New(newTickingClock())
	})
}

type tickingClock struct {
	v *clock.Virtual
}

func newTickingClock() *tickingClock {
	return &tickingClock{v: clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))}
}

func (c *tickingClock) Now() time.Time {
	c.v.Advance(time.Millisecond)
	return c.v.Now()
}
