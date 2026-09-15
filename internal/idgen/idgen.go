// Package idgen provides the implementations of core.IDGen.
//
// Random generates UUIDv4 for production. Sequential generates predictable
// identifiers for tests, so that a failing run can be described by the same
// IDs every time it is reproduced.
//
// Identity enters the runtime here or not at all. Nothing above the
// composition root generates an ID for itself.
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Random issues UUIDv4 identifiers from crypto/rand.
//
// Implemented directly rather than pulled in as a dependency: the whole of it
// is sixteen random bytes with six of them masked, and a runtime whose
// identity source is this small should not need a module for it.
type Random struct{}

func (Random) NewRunID() core.RunID   { return core.RunID(uuidv4()) }
func (Random) NewTaskID() core.TaskID { return core.TaskID(uuidv4()) }

func uuidv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the OS entropy source is broken. There is
		// no correct way to continue: every identity this runtime issues from
		// here would be suspect.
		panic(fmt.Sprintf("idgen: crypto/rand unavailable: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}

// Sequential issues predictable identifiers: run-1, task-1, task-2, ...
//
// The IDs are not UUIDs and are not meant to be. They are for tests, where a
// readable identifier in a failure message is worth more than a realistic one.
type Sequential struct {
	runs  atomic.Int64
	tasks atomic.Int64
}

func NewSequential() *Sequential { return &Sequential{} }

func (s *Sequential) NewRunID() core.RunID {
	return core.RunID(fmt.Sprintf("run-%d", s.runs.Add(1)))
}

func (s *Sequential) NewTaskID() core.TaskID {
	return core.TaskID(fmt.Sprintf("task-%d", s.tasks.Add(1)))
}
