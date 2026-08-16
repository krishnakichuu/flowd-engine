// Package helloworkflow is flowd's minimal end-to-end example: one
// workflow calling one activity. It doubles as the Phase 1 milestone —
// see test/integration/crash_recovery_test.go, which starts this workflow,
// kills a worker mid-flight, restarts it, and asserts it completes exactly
// once via deterministic replay (ADR-0001).
package helloworkflow

import (
	"fmt"
	"os"
	"time"

	"github.com/krishnakichuu/flowd/sdk/activity"
	"github.com/krishnakichuu/flowd/sdk/workflow"
)

// TaskQueue is the task queue this example's worker and starter agree on.
const TaskQueue = "helloworkflow"

// SimpleWorkflow calls GreetActivity once and returns its result.
//
// Reading HELLOWORKFLOW_ACTIVITY_TIMEOUT here is safe under replay even
// though it's an env var: it only affects the ActivityOptions passed on
// the *first* schedule of a given activity (an already-scheduled activity,
// on replay, returns early without re-reading options — see
// replayer.Execution.ScheduleActivity), and it isn't part of the
// non-determinism check, which compares only activity type and position.
// It exists so test/integration/crash_recovery_test.go can force a short
// lease without changing this example's normal zero-config behavior.
func SimpleWorkflow(ctx workflow.Context, name string) (string, error) {
	opts := workflow.ActivityOptions{}
	if v := os.Getenv("HELLOWORKFLOW_ACTIVITY_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			opts.StartToCloseTimeout = d
		}
	}

	var greeting string
	err := workflow.ExecuteActivity(ctx, GreetActivity, name, opts).Get(&greeting)
	if err != nil {
		return "", err
	}
	return greeting, nil
}

// GreetActivity performs the (here, trivial) side effect a real activity
// would: something a workflow function is not allowed to do directly.
//
// Two env vars exist purely for test/integration/crash_recovery_test.go,
// which cannot observe an activity's execution count any other way once it
// runs in a separate, killable OS process: HELLOWORKFLOW_COUNTER_FILE (if
// set, appends one line per invocation — a side channel independent of
// history) and HELLOWORKFLOW_ACTIVITY_DELAY (if set, sleeps before
// returning, giving the test a wide, deterministic window to kill the
// worker mid-execution). Both are no-ops when unset, which is always true
// outside that test.
func GreetActivity(_ activity.Context, name string) (string, error) {
	if f := os.Getenv("HELLOWORKFLOW_COUNTER_FILE"); f != "" {
		// #nosec G304 G703 -- test-only path, supplied by the test process
		// itself via env var, never external/attacker input.
		if fh, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			_, _ = fmt.Fprintln(fh, "1")
			_ = fh.Close()
		}
	}
	if v := os.Getenv("HELLOWORKFLOW_ACTIVITY_DELAY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			time.Sleep(d)
		}
	}
	return fmt.Sprintf("Hello, %s!", name), nil
}
