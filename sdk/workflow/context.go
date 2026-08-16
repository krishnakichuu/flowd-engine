// Package workflow provides the deterministic primitives workflow code is
// allowed to use: Now, Sleep, ExecuteActivity, and Go. This restricted
// surface is what makes replay possible (ADR-0001) — workflow functions
// must not perform direct I/O, call time.Now(), spawn raw goroutines, or
// use unmediated randomness; anything a workflow needs from the outside
// world goes through one of these.
package workflow

import (
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
	"github.com/krishnakichuu/flowd/sdk/internal/replayer"
)

// Context is passed to every workflow function and coroutine started with
// Go. It is not a context.Context — workflow code has no business doing
// direct I/O or cancellation-by-deadline outside the deterministic
// primitives below.
type Context struct {
	exec *replayer.Execution
	co   *replayer.Coroutine
}

// NewContext constructs a workflow Context. It is exported for
// sdk/worker's use when driving a workflow task; workflow authors never
// call it directly — the SDK constructs it for them.
func NewContext(exec *replayer.Execution, co *replayer.Coroutine) Context {
	return Context{exec: exec, co: co}
}

// Now returns the time the current workflow task started, recorded in
// history — never wall-clock — so replay reconstructs the exact same value
// every time (ADR-0001).
func Now(ctx Context) time.Time {
	return ctx.exec.Now
}

// Sleep blocks the calling coroutine until the given duration has elapsed
// according to the workflow's timeline (a server-fired timer), not
// wall-clock time.
func Sleep(ctx Context, d time.Duration) {
	f := ctx.exec.ScheduleTimer(d)
	f.Get(ctx.co)
}

// Go starts a new cooperatively-scheduled coroutine. Unlike a raw
// goroutine, its scheduling is deterministic across replay because it only
// ever runs to completion or to its next Yield point when the dispatcher
// resumes it.
func Go(ctx Context, fn func(Context)) {
	ctx.exec.Dispatcher.Go(func(c *replayer.Coroutine) {
		fn(NewContext(ctx.exec, c))
	})
}

// ActivityOptions configures a single ExecuteActivity call.
type ActivityOptions struct {
	RetryPolicy            *flowv1.RetryPolicy
	ScheduleToStartTimeout time.Duration
	StartToCloseTimeout    time.Duration
}

// Future is returned by ExecuteActivity; Get blocks until the activity's
// result is known and unmarshals it into resultPtr via the default
// DataConverter.
type Future struct {
	co *replayer.Coroutine
	f  *replayer.ActivityFuture
}

// Get blocks until the activity completes or fails, unmarshaling a
// successful result into resultPtr (pass nil if the activity returns
// nothing of interest).
func (fut Future) Get(resultPtr any) error {
	result, failure := fut.f.Get(fut.co)
	if failure != nil {
		return &ActivityError{Type: failure.Type, Message: failure.Message}
	}
	if resultPtr == nil {
		return nil
	}
	return converter.Default.FromPayload(result, resultPtr)
}

// ActivityError wraps a terminal activity failure surfaced to workflow
// code via Future.Get.
type ActivityError struct {
	Type    string
	Message string
}

func (e *ActivityError) Error() string { return "activity error (" + e.Type + "): " + e.Message }
