package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

// The effect ledger, from an operator's side.
//
// This exists because the runtime refuses to guess. When a provider can
// neither deduplicate nor answer, the honest outcome is an effect parked in
// UNKNOWN and a person deciding what happened — and a design that stops there
// without giving the person a way to look, and a way to record their decision,
// has only moved the silent failure somewhere else.

func cmdEffects(args []string) error {
	if len(args) == 0 {
		return cmdEffectsList(nil)
	}
	switch args[0] {
	case "list":
		return cmdEffectsList(args[1:])
	case "show":
		return cmdEffectsShow(args[1:])
	case "resolve":
		return cmdEffectsResolve(args[1:])
	default:
		// Bare `veya effects --dsn ...` lists, so flags fall through.
		if args[0][0] == '-' {
			return cmdEffectsList(args)
		}
		return fmt.Errorf("effects: unknown subcommand %q", args[0])
	}
}

func cmdEffectsList(args []string) error {
	fs := flag.NewFlagSet("veya effects list", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	runID := fs.String("run", "", "list every effect of one run instead of unresolved ones")
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

	var (
		rows  []core.Effect
		empty string
	)
	if *runID != "" {
		rows, err = store.ListEffects(ctx, core.RunID(*runID))
		empty = fmt.Sprintf("run %s has no effects", *runID)
	} else {
		// Everything older than now, which is everything: an operator asking
		// what is outstanding wants the full picture, not a sample.
		rows, err = store.UnresolvedEffects(ctx, time.Now(), *limit)
		empty = "no unresolved effects"
	}
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Println(empty)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tTOOL\tCLASS\tSTATUS\tAGE\tEXTERNAL REF")
	for _, e := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			e.Key, e.Type, e.Class, e.Status,
			time.Since(e.CreatedAt).Truncate(time.Second), e.ExternalRef)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	if *runID == "" {
		fmt.Fprintf(os.Stderr, "\n%d unresolved. UNRECONCILABLE outcomes will not resolve "+
			"on their own — settle one with:\n  veya effects resolve KEY --committed --ref REF\n",
			len(rows))
	}
	return nil
}

func cmdEffectsShow(args []string) error {
	fs := flag.NewFlagSet("veya effects show", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("effects show: expected exactly one IDEMPOTENCY_KEY")
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	e, err := store.GetEffect(ctx, core.IdempotencyKey(fs.Arg(0)))
	if err != nil {
		return err
	}

	fmt.Printf("key           %s\n", e.Key)
	fmt.Printf("run           %s\n", e.RunID)
	fmt.Printf("task          %s\n", e.TaskID)
	fmt.Printf("tool          %s\n", e.Type)
	fmt.Printf("class         %s\n", e.Class)
	fmt.Printf("status        %s\n", e.Status)
	fmt.Printf("created       %s\n", e.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("updated       %s\n", e.UpdatedAt.Format("2006-01-02 15:04:05"))
	if e.CommittedAt != nil {
		fmt.Printf("committed     %s\n", e.CommittedAt.Format("2006-01-02 15:04:05"))
	}
	if e.ExternalRef != "" {
		fmt.Printf("external ref  %s\n", e.ExternalRef)
	}
	if len(e.Request) > 0 {
		fmt.Printf("request       %s\n", e.Request)
	}
	if len(e.Response) > 0 {
		fmt.Printf("response      %s\n", e.Response)
	}
	if e.LastError != "" {
		fmt.Printf("last error    %s\n", e.LastError)
	}

	if e.Status == core.EffectUnknown {
		fmt.Fprintf(os.Stderr, "\nThis outcome is genuinely unknown. Check the provider "+
			"for %s, then record what you find:\n"+
			"  veya effects resolve %s --committed --ref THEIR_REFERENCE\n"+
			"  veya effects resolve %s --not-executed\n", e.Key, e.Key, e.Key)
	}
	return nil
}

// cmdEffectsResolve records a human's finding about an effect.
//
// The operator is asserting a fact they established by looking at the
// provider, which is why the two flags are mutually exclusive and neither has
// a default. There is no --probably.
func cmdEffectsResolve(args []string) error {
	fs := flag.NewFlagSet("veya effects resolve", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	committed := fs.Bool("committed", false, "the action did happen")
	notExecuted := fs.Bool("not-executed", false, "the action did not happen")
	ref := fs.String("ref", "", "the provider's own identifier, when it happened")
	note := fs.String("note", "", "how you established this")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("effects resolve: expected exactly one IDEMPOTENCY_KEY")
	}
	switch {
	case *committed == *notExecuted:
		return errors.New("effects resolve: pass exactly one of --committed or --not-executed")
	case *committed && *ref == "":
		return errors.New("effects resolve: --committed needs --ref, the provider's identifier for what happened")
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	key := core.IdempotencyKey(fs.Arg(0))
	existing, err := store.GetEffect(ctx, key)
	if err != nil {
		return err
	}
	if existing.Status.IsResolved() {
		return fmt.Errorf("effect %s is already %s; it needs no resolution", key, existing.Status)
	}

	target := core.EffectFailed
	detail := "operator confirmed this never happened"
	if *committed {
		target = core.EffectCommitted
		detail = fmt.Sprintf("operator confirmed this happened (%s)", *ref)
	}
	if *note != "" {
		detail = fmt.Sprintf("%s: %s", detail, *note)
	}

	err = store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		// A RUNNING row has to admit it is unknown before it can be settled,
		// which keeps the recorded history of the effect honest rather than
		// jumping straight from "in progress" to a human's conclusion.
		from := existing.Status
		if from == core.EffectRunning {
			if err := tx.TransitionEffect(ctx, key, core.EffectRunning, core.EffectUnknown,
				core.EffectOutcome{Error: "owner stopped without reporting an outcome"}); err != nil {
				return err
			}
			from = core.EffectUnknown
		}

		out := core.EffectOutcome{ExternalRef: *ref}
		if target == core.EffectFailed {
			out.Error = detail
		}
		if err := tx.TransitionEffect(ctx, key, from, target, out); err != nil {
			return err
		}

		resolved := existing
		resolved.Status = target
		resolved.ExternalRef = *ref
		return core.Append(ctx, tx, existing.RunID, core.EventEffectReconciled, "", core.EffectData{
			TaskID:      resolved.TaskID,
			Key:         resolved.Key,
			EffectType:  resolved.Type,
			Class:       resolved.Class,
			Status:      target,
			ExternalRef: *ref,
			Detail:      detail,
		})
	})
	if err != nil {
		return fmt.Errorf("resolve effect %s: %w", key, err)
	}

	fmt.Printf("%s -> %s\n", key, target)
	fmt.Fprintf(os.Stderr, "recorded in run %s history\n", existing.RunID)
	return nil
}
