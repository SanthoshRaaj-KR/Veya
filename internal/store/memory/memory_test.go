package memory_test

import (
	"testing"

	"github.com/santhoshraajkr/veya/internal/core"
	"github.com/santhoshraajkr/veya/internal/core/storetest"
	"github.com/santhoshraajkr/veya/internal/store/memory"
)

// The memory adapter runs the same suite as PostgreSQL. If the two ever
// disagree, one of them is wrong — that is the point of the suite existing.
func TestStoreContract(t *testing.T) {
	storetest.RunStoreSuite(t, func(t *testing.T) core.Store {
		return memory.New(storetest.TickingClock())
	})
}
