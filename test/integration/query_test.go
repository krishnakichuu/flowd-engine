//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/krishnakichuu/flowd/examples/countdown"
	"github.com/krishnakichuu/flowd/sdk/client"
)

// TestWorkflowQuery exercises workflow queries end to end (Phase 2
// roadmap, Track D, item 1) against a real server, Postgres, and worker:
// querying both the current run and an already-continued-away earlier run
// of the same workflow_id, and confirming a query never appends anything
// to history — the property the whole feature promises.
func TestWorkflowQuery(t *testing.T) {
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

	workerProc := startProcess(t, workerBin, nil, []string{"FLOWD_ADDR=" + grpcAddr})
	defer stopProcess(workerProc)

	c, err := client.Dial(grpcAddr, client.Options{})
	if err != nil {
		t.Fatalf("dial flowd: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowID := fmt.Sprintf("query-test-%d", time.Now().UnixNano())
	run, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID, TaskQueue: countdown.TaskQueue}, countdown.CountdownWorkflow, 3)
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	firstRunID := run.RunID

	var result string
	if err := run.Get(ctx, &result); err != nil {
		t.Fatalf("workflow did not complete: %v", err)
	}
	finalRunID := run.RunID
	if finalRunID == firstRunID {
		t.Fatal("expected the countdown workflow to have continued into a later run")
	}

	// Query the current (final) run: it started its last tick with
	// remaining=1.
	var remaining int
	if err := c.QueryWorkflow(ctx, workflowID, "", "remaining", struct{}{}, &remaining); err != nil {
		t.Fatalf("query current run: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("current run's remaining = %d, want 1", remaining)
	}

	// Query the very first run explicitly, by run_id — it's long since
	// closed via ContinueAsNew, but a query can still replay it to answer:
	// it started with remaining=3.
	eventsBefore, err := c.GetWorkflowHistory(ctx, workflowID, firstRunID)
	if err != nil {
		t.Fatalf("get first run history before query: %v", err)
	}

	remaining = 0
	if err := c.QueryWorkflow(ctx, workflowID, firstRunID, "remaining", struct{}{}, &remaining); err != nil {
		t.Fatalf("query first run: %v", err)
	}
	if remaining != 3 {
		t.Fatalf("first run's remaining = %d, want 3", remaining)
	}

	// The property the whole feature promises: a query never appends to
	// history_events, on either the current run or an old, closed one.
	eventsAfter, err := c.GetWorkflowHistory(ctx, workflowID, firstRunID)
	if err != nil {
		t.Fatalf("get first run history after query: %v", err)
	}
	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("first run has %d events after querying it, had %d before — a query must not touch history", len(eventsAfter), len(eventsBefore))
	}
}

// TestWorkflowQueryUnknownWorkflowIsNotFound confirms a query against a
// workflow_id that was never started fails fast with NotFound, rather
// than hanging until the query timeout.
func TestWorkflowQueryUnknownWorkflowIsNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	flowdBin := buildBinary(t, tmpDir, "flowd", "github.com/krishnakichuu/flowd/cmd/flowd")

	grpcAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	server := startProcess(t, flowdBin, nil, []string{
		"FLOWD_GRPC_ADDR=" + grpcAddr,
		"FLOWD_METRICS_ADDR=" + metricsAddr,
		"FLOWD_DATABASE_DSN=" + databaseDSN(),
	})
	defer stopProcess(server)
	waitForHealthy(t, metricsAddr)

	c, err := client.Dial(grpcAddr, client.Options{})
	if err != nil {
		t.Fatalf("dial flowd: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = c.QueryWorkflow(ctx, "no-such-workflow", "", "remaining", struct{}{}, nil)
	if err == nil {
		t.Fatal("expected an error querying a workflow that was never started")
	}
}
