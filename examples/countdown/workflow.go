// Package countdown is flowd's ContinueAsNew example: a workflow that
// ticks once per run, then continues as new with one fewer tick
// remaining, instead of looping within a single run whose history would
// otherwise grow without bound (see Phase 2 roadmap, Track B, item 2). It
// doubles as test/integration/continue_as_new_test.go's fixture.
package countdown

import (
	"fmt"
	"os"

	"github.com/krishnakichuu/flowd/sdk/activity"
	"github.com/krishnakichuu/flowd/sdk/workflow"
)

// TaskQueue is the task queue this example's worker and starter agree on.
const TaskQueue = "countdown"

// CountdownWorkflow calls TickActivity once, then either continues as new
// with remaining-1 (if there's still work left) or completes. Each
// continuation is a fresh run with its own fresh history — replaying this
// workflow to resume it never has to replay more than one tick's worth of
// events, no matter how large the original remaining count was.
//
// It also registers a "remaining" query handler — see
// test/integration/query_test.go for a live example of asking a running
// execution "what step are you on" without waiting for it to finish.
func CountdownWorkflow(ctx workflow.Context, remaining int) (string, error) {
	if err := workflow.SetQueryHandler(ctx, "remaining", func(struct{}) (int, error) {
		return remaining, nil
	}); err != nil {
		return "", err
	}

	var tick string
	if err := workflow.ExecuteActivity(ctx, TickActivity, remaining, workflow.ActivityOptions{}).Get(&tick); err != nil {
		return "", err
	}
	if remaining <= 1 {
		return "done", nil
	}
	return "", workflow.NewContinueAsNewError(CountdownWorkflow, remaining-1, workflow.ContinueAsNewOptions{})
}

// TickActivity performs the one side effect a real activity would.
//
// COUNTDOWN_COUNTER_FILE exists purely for
// test/integration/continue_as_new_test.go, which cannot observe how many
// times this ran across separate continue-as-new runs any other way once
// it executes in a separate OS process: if set, it appends one line per
// invocation — a side channel independent of any single run's history.
// It's a no-op when unset, which is always true outside that test.
func TickActivity(_ activity.Context, remaining int) (string, error) {
	if f := os.Getenv("COUNTDOWN_COUNTER_FILE"); f != "" {
		// #nosec G304 G703 -- test-only path, supplied by the test process
		// itself via env var, never external/attacker input.
		if fh, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			_, _ = fmt.Fprintln(fh, remaining)
			_ = fh.Close()
		}
	}
	return fmt.Sprintf("tick %d", remaining), nil
}
