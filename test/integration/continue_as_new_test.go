//go:build integration

// See crash_recovery_test.go for this package's shared harness helpers
// (buildBinary, startProcess, freeAddr, waitForHealthy, ...).
package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/examples/countdown"
	"github.com/krishnakichuu/flowd/sdk/client"
)

// TestContinueAsNew starts a CountdownWorkflow with 3 remaining ticks and
// confirms it actually produces 3 separate runs — closing each one with a
// WorkflowExecutionContinuedAsNew event and starting the next atomically —
// rather than one run looping internally, and that the client-side Get()
// transparently follows the whole chain to the real final result (see
// sdk/client.WorkflowRun.Get's ContinueAsNew handling).
func TestContinueAsNew(t *testing.T) {
	tmpDir := t.TempDir()
	flowdBin := buildBinary(t, tmpDir, "flowd", "github.com/krishnakichuu/flowd/cmd/flowd")
	workerBin := buildBinary(t, tmpDir, "countdown-worker", "github.com/krishnakichuu/flowd/examples/countdown/worker")

	grpcAddr := freeAddr(t)
	metricsAddr := freeAddr(t)

	server := startProcess(t, flowdBin, nil, []string{
		"FLOWD_GRPC_ADDR=" + grpcAddr,
		"FLOWD_METRICS_ADDR=" + metricsAddr,
		"FLOWD_DATABASE_DSN=" + databaseDSN(),
	})
	defer stopProcess(server)
	waitForHealthy(t, metricsAddr)

	counterFile := filepath.Join(tmpDir, "ticks.txt")
	workerProc := startProcess(t, workerBin, nil, []string{
		"FLOWD_ADDR=" + grpcAddr,
		"COUNTDOWN_COUNTER_FILE=" + counterFile,
	})
	defer stopProcess(workerProc)

	c, err := client.Dial(grpcAddr, client.Options{})
	if err != nil {
		t.Fatalf("dial flowd: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowID := fmt.Sprintf("continue-as-new-test-%d", time.Now().UnixNano())
	run, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID, TaskQueue: countdown.TaskQueue}, countdown.CountdownWorkflow, 3)
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	firstRunID := run.RunID

	var result string
	if err := run.Get(ctx, &result); err != nil {
		t.Fatalf("workflow did not complete: %v", err)
	}
	if result != "done" {
		t.Fatalf("result = %q, want %q", result, "done")
	}
	if run.RunID == firstRunID {
		t.Fatal("Get should have followed the continuation chain to a later run, not settled on the first run")
	}
	finalRunID := run.RunID

	// 3 remaining -> 3 separate runs, each ticking exactly once — proves
	// the activity really ran 3 times across 3 distinct histories, not
	// once in a single run's internal loop.
	assertCounterLines(t, counterFile, 3)

	// The workflow_id's current execution should resolve (empty run_id) to
	// the same final run Get() settled on, and be COMPLETED.
	desc, err := c.DescribeWorkflow(ctx, workflowID, "")
	if err != nil {
		t.Fatalf("describe current execution: %v", err)
	}
	if desc.Execution.RunId != finalRunID {
		t.Fatalf("current_executions resolves to run_id %q, want %q (Get's final run)", desc.Execution.RunId, finalRunID)
	}
	if desc.Status != flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("final run status = %s, want COMPLETED", desc.Status)
	}

	// The very first run closed via ContinueAsNew, not a normal
	// completion, and its terminal event names the run that replaced it.
	firstRunEvents, err := c.GetWorkflowHistory(ctx, workflowID, firstRunID)
	if err != nil {
		t.Fatalf("get first run history: %v", err)
	}
	assertEventCounts(t, firstRunEvents, map[flowv1.HistoryEventType]int{
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:          1,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:        0,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: 1,
	})
	var continuedTo string
	for _, ev := range firstRunEvents {
		if a, ok := ev.Attributes.(*flowv1.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes); ok {
			continuedTo = a.WorkflowExecutionContinuedAsNewEventAttributes.NewRunId
		}
	}
	if continuedTo == "" {
		t.Fatal("first run's ContinuedAsNew event did not record a new_run_id")
	}

	// The run it continued into starts its own, fresh history — not a
	// continuation of the first run's event count — and records where it
	// came from.
	secondRunEvents, err := c.GetWorkflowHistory(ctx, workflowID, continuedTo)
	if err != nil {
		t.Fatalf("get second run history: %v", err)
	}
	if len(secondRunEvents) == 0 || secondRunEvents[0].EventType != flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		t.Fatalf("second run's history should start fresh with WorkflowExecutionStarted, got %v", secondRunEvents)
	}
	startedAttrs, ok := secondRunEvents[0].Attributes.(*flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes)
	if !ok {
		t.Fatalf("second run's event 1 attributes are %T, want WorkflowExecutionStartedEventAttributes", secondRunEvents[0].Attributes)
	}
	if got := startedAttrs.WorkflowExecutionStartedEventAttributes.ContinuedFromRunId; got != firstRunID {
		t.Fatalf("second run's continued_from_run_id = %q, want %q", got, firstRunID)
	}

	// The final run closed normally, not via another continuation.
	finalRunEvents, err := c.GetWorkflowHistory(ctx, workflowID, finalRunID)
	if err != nil {
		t.Fatalf("get final run history: %v", err)
	}
	assertEventCounts(t, finalRunEvents, map[flowv1.HistoryEventType]int{
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:        1,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: 0,
	})
}
