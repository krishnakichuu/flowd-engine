//go:build integration

// Package integration holds tests that exercise real binaries against a
// real Postgres instance (see Makefile's test-integration target). This
// file is the Phase 1 milestone: start a workflow, kill a worker while an
// activity is genuinely mid-execution, restart a worker, and confirm the
// workflow completes correctly via deterministic replay (ADR-0001) and
// that crash-recovery redispatch behaved exactly as ADR-0002 predicts.
package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/examples/helloworkflow"
	"github.com/krishnakichuu/flowd/sdk/client"
)

const testDatabaseDSN = "postgres://flowd:flowd@localhost:5432/flowd?sslmode=disable"

func TestCrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	flowdBin := buildBinary(t, tmpDir, "flowd", "github.com/krishnakichuu/flowd/cmd/flowd")
	workerBin := buildBinary(t, tmpDir, "helloworkflow-worker", "github.com/krishnakichuu/flowd/examples/helloworkflow/worker")

	grpcAddr := freeAddr(t)
	metricsAddr := freeAddr(t)

	server := startProcess(t, flowdBin, nil, []string{
		"FLOWD_GRPC_ADDR=" + grpcAddr,
		"FLOWD_METRICS_ADDR=" + metricsAddr,
		"FLOWD_DATABASE_DSN=" + databaseDSN(),
		"FLOWD_REAPER_INTERVAL=1s",
	})
	defer stopProcess(server)
	waitForHealthy(t, metricsAddr)

	counterFile := filepath.Join(tmpDir, "counter.txt")
	workerEnv := []string{
		"FLOWD_ADDR=" + grpcAddr,
		"HELLOWORKFLOW_COUNTER_FILE=" + counterFile,
		"HELLOWORKFLOW_ACTIVITY_DELAY=2s",
		"HELLOWORKFLOW_ACTIVITY_TIMEOUT=5s",
	}
	worker1 := startProcess(t, workerBin, nil, workerEnv)

	c, err := client.Dial(grpcAddr, client.Options{})
	if err != nil {
		t.Fatalf("dial flowd: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowID := fmt.Sprintf("crash-recovery-test-%d", time.Now().UnixNano())
	run, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID, TaskQueue: helloworkflow.TaskQueue}, helloworkflow.SimpleWorkflow, "IntegrationTest")
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	// Wait until the activity is genuinely executing (ActivityTaskStarted
	// recorded) before killing worker 1 — HELLOWORKFLOW_ACTIVITY_DELAY=2s
	// gives a wide, deterministic window to land the kill mid-execution
	// rather than racing a near-instant activity.
	waitForEvent(t, ctx, c, workflowID, run.RunID, flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_STARTED, 1)

	if err := worker1.Process.Kill(); err != nil {
		t.Fatalf("kill worker1: %v", err)
	}
	_, _ = worker1.Process.Wait()

	// Nothing is polling for ~1-2s here (until the lease reaper reclaims
	// the orphaned activity task) — this is the window during which the
	// workflow can only make progress once a worker resumes it.

	worker2 := startProcess(t, workerBin, nil, []string{
		"FLOWD_ADDR=" + grpcAddr,
		"HELLOWORKFLOW_COUNTER_FILE=" + counterFile,
	})
	defer stopProcess(worker2)

	var result string
	if err := run.Get(ctx, &result); err != nil {
		t.Fatalf("workflow did not complete: %v", err)
	}
	if result != "Hello, IntegrationTest!" {
		t.Fatalf("unexpected result: %q", result)
	}

	// Activity ran exactly twice: once killed mid-flight (its result was
	// never reported, so the server never recorded completion), once after
	// the reaper reclaimed the lease and worker 2 redispatched it — this
	// is exactly the at-least-once redelivery ADR-0002's lease reaper
	// exists to guarantee, made observable via a side channel independent
	// of history.
	assertCounterLines(t, counterFile, 2)

	events, err := c.GetWorkflowHistory(ctx, workflowID, run.RunID)
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	assertEventCounts(t, events, map[flowv1.HistoryEventType]int{
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:   1,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:      1,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_STARTED:        2,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_COMPLETED:      1,
		flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED: 1,
	})
}

func databaseDSN() string {
	if v := os.Getenv("FLOWD_TEST_DATABASE_DSN"); v != "" {
		return v
	}
	return testDatabaseDSN
}

func buildBinary(t *testing.T, dir, name, pkg string) string {
	t.Helper()
	out := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Join(wd, "..", "..")
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func startProcess(t *testing.T, bin string, args, env []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	return cmd
}

func stopProcess(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

func waitForHealthy(t *testing.T, metricsAddr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + metricsAddr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("flowd did not become healthy at %s", metricsAddr)
}

func waitForEvent(t *testing.T, ctx context.Context, c *client.Client, workflowID, runID string, eventType flowv1.HistoryEventType, count int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := c.GetWorkflowHistory(ctx, workflowID, runID)
		if err == nil {
			n := 0
			for _, ev := range events {
				if ev.EventType == eventType {
					n++
				}
			}
			if n >= count {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d x %s", count, eventType)
}

func assertCounterLines(t *testing.T, path string, want int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read counter file: %v", err)
	}
	got := len(strings.Fields(string(data)))
	if got != want {
		t.Fatalf("activity execution count = %d, want %d", got, want)
	}
}

func assertEventCounts(t *testing.T, events []*flowv1.HistoryEvent, want map[flowv1.HistoryEventType]int) {
	t.Helper()
	got := make(map[flowv1.HistoryEventType]int)
	for _, ev := range events {
		got[ev.EventType]++
	}
	for evType, wantCount := range want {
		if got[evType] != wantCount {
			t.Errorf("event %s count = %d, want %d", evType, got[evType], wantCount)
		}
	}
}
