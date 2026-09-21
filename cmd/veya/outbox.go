package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

// The outbox, from an operator's side.
//
// This list is normally empty, and that is what makes it useful. A row here is
// a task that is committed and has not reached a worker, so anything that
// persists across a few seconds means the relay is stopped or the broker is
// refusing work — which is the difference between "the run is slow" and "the
// run will never move".

func cmdOutbox(args []string) error {
	fs := flag.NewFlagSet("veya outbox", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	limit := fs.Int("limit", 50, "maximum rows to show")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	now := time.Now()
	pending, err := store.PendingDeliveries(ctx, now, *limit)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		fmt.Println("no undelivered work ready now")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTASK\tAGE\tATTEMPTS\tLAST ERROR")
	for _, d := range pending {
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\n",
			d.ID, d.TaskID, time.Since(d.CreatedAt).Truncate(time.Second),
			d.Attempts, d.LastError)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// A few rows a second old is the relay working. The same rows a minute
	// later, with attempts climbing, is the thing worth waking up for. A
	// retry backing off on schedule is not here at all: PendingDeliveries
	// only returns rows ready now, so this list stays a health check rather
	// than something an operator has to filter for themselves.
	fmt.Fprintf(os.Stderr, "\n%d committed, ready, and not yet delivered. This list is "+
		"normally empty; rows that persist mean no relay is running, or the broker is "+
		"refusing work.\n", len(pending))
	return nil
}
