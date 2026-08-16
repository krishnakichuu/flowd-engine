package workflow

import (
	"fmt"
	"reflect"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
)

// SetSignalHandler registers handler — a func(In) — to act on
// SignalWorkflowExecution calls of signalName delivered into this
// workflow's history as a WorkflowExecutionSignaled event (Phase 2
// roadmap item: signal dispatch). Unlike SetQueryHandler, a signal
// handler has no return value: signaling is asynchronous and
// fire-and-forget by design (see client.Client.SignalWorkflow, which does
// not wait for or expose one).
//
// Like a query handler, it must not block — no ExecuteActivity, Sleep, or
// anything else that yields from inside one (see
// replayer.Execution.SetSignalHandler's doc for why: delivery runs from a
// dedicated internal coroutine, and a handler that blocked there would
// stall every other signal queued behind it). A workflow that needs to
// *wait* for a signal should have the handler record what happened (e.g.
// set a field) for the main workflow coroutine to observe on its own
// Yield-driven checks — the same pattern IsCancelRequested already
// establishes for cancellation.
//
// Call this once, near the top of the workflow function, before its
// first blocking call — same reasoning as SetQueryHandler: the very
// first ExecuteRound already runs the workflow up to that first blocking
// point, registering the handler well before any signal recorded earlier
// in this same history can be delivered to it, on a fresh replay or a
// sticky resume alike.
func SetSignalHandler(ctx Context, signalName string, handler any) error {
	v := reflect.ValueOf(handler)
	t := v.Type()
	if t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 0 {
		return fmt.Errorf("workflow: signal handler must be func(In), got %s", t)
	}

	ctx.exec.SetSignalHandler(signalName, func(args *flowv1.Payload) {
		inPtr := reflect.New(t.In(0))
		if err := converter.Default.FromPayload(args, inPtr.Interface()); err != nil {
			// No reply channel to report this through (unlike a query's
			// answer) — a malformed payload is a real bug (a sender and
			// this workflow disagreeing on the signal's shape), so this
			// fails loudly the same way a mismatched activity/timer
			// replay does: a panic caught by the coroutine's own recover
			// and surfaced via Dispatcher.FirstPanic, not silently
			// dropped.
			panic(fmt.Errorf("workflow: signal %q: unmarshal args: %w", signalName, err))
		}
		v.Call([]reflect.Value{inPtr.Elem()})
	})
	return nil
}
