// Package core defines Veya's domain types and the port interfaces every
// adapter implements. It is the innermost layer: the engine depends on it,
// adapters implement it, and nothing here depends on anything else.
//
// # Dependency rule
//
// This package imports nothing from internal/. It must never import a database
// driver, a broker client, or net/http. If a type from an adapter appears in a
// signature here, the boundary has leaked and the port is wrong.
//
// The practical payoff: Layer 3 swaps task delivery from PostgreSQL to NATS
// JetStream by adding one package and changing one line in cmd/. Nothing in
// core or engine moves. If that turns out not to be true, the abstraction was
// wrong and the fix belongs here, not in a workaround upstream.
//
// # What lives here
//
//	ids.go        RunID, TaskID, StepID — the identity types
//	run.go        Run and its state machine
//	task.go       Task and its state machine
//	event.go      Event, event types, and the versioned payload envelope
//	effect.go     the effect ledger: statuses, idempotency keys, UNKNOWN
//	tool.go       ToolDescriptor, effect classes, Reconciler
//	lease.go      leases, fencing tokens, the recovery decision table
//	outbox.go     delivery intent: the row that closes the dual write
//	decision.go   Decider — what an agent does next, independent of how
//	errors.go     Sentinel errors every adapter translates into
//	ports.go      Store, Tx, Dispatcher, Clock, IDGen
//
// # Time and identity are injected
//
// There is no time.Now or crypto/rand call anywhere above the composition
// root. Both arrive as ports (Clock, IDGen) because a durable execution
// runtime that cannot be replayed deterministically cannot be tested, and
// retrofitting that after the fact is a rewrite rather than a refactor.
package core
