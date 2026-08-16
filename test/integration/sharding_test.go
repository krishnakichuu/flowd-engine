//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/krishnakichuu/flowd/internal/history"
	postgrespkg "github.com/krishnakichuu/flowd/internal/persistence/postgres"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// TestTaskQueuePartitionDispatchIsolation exercises the actual routing
// layer (Phase 2 roadmap, Track C, item 3) against real Postgres: two
// tasks land in different partitions (history.PartitionFor is
// deterministic, so this picks two workflow_ids that hash to different
// partitions out of a small partition count), and a poller restricted to
// one partition only ever sees its own — never the other's, and never
// nothing when its own is available.
func TestTaskQueuePartitionDispatchIsolation(t *testing.T) {
	ctx := context.Background()
	pool, err := postgrespkg.NewPool(ctx, databaseDSN())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	q := sqlc.New(pool)
	ns, err := q.GetNamespaceByName(ctx, "default")
	if err != nil {
		t.Fatalf("get default namespace: %v", err)
	}

	const numPartitions = 4
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	taskQueue := "partition-test-queue-" + suffix

	// Find two workflow_ids that land in different partitions — with only
	// 4 partitions this is virtually guaranteed within a handful of tries,
	// but loop defensively rather than assume the first attempt works.
	var wfA, wfB string
	var partA, partB int32
	for i := 0; ; i++ {
		wfA = fmt.Sprintf("wf-a-%s-%d", suffix, i)
		wfB = fmt.Sprintf("wf-b-%s-%d", suffix, i)
		partA = history.PartitionFor(wfA, numPartitions)
		partB = history.PartitionFor(wfB, numPartitions)
		if partA != partB {
			break
		}
		if i > 50 {
			t.Fatal("could not find two workflow_ids landing in different partitions after 50 tries")
		}
	}

	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: ns.ID, TaskQueueName: taskQueue, TaskQueuePartition: partA,
		WorkflowID: wfA, RunID: "run-a", ScheduledEventID: 1,
	}); err != nil {
		t.Fatalf("enqueue task for partition A: %v", err)
	}
	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: ns.ID, TaskQueueName: taskQueue, TaskQueuePartition: partB,
		WorkflowID: wfB, RunID: "run-b", ScheduledEventID: 1,
	}); err != nil {
		t.Fatalf("enqueue task for partition B: %v", err)
	}

	// A poller restricted to partition A must get wfA's task, never wfB's.
	row, err := q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		Partitions: []int32{partA},
	})
	if err != nil {
		t.Fatalf("dequeue restricted to partition A: %v", err)
	}
	if row.WorkflowID != wfA {
		t.Fatalf("partition-A poller got workflow_id %q, want %q (partition isolation violated)", row.WorkflowID, wfA)
	}

	// That same poller must not see wfB's task even though it's the only
	// one left pending — it belongs to a partition this poller doesn't serve.
	_, err = q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		Partitions: []int32{partA},
	})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("partition-A poller dequeue for wfB's task = %v, want pgx.ErrNoRows", err)
	}

	// A poller restricted to partition B gets exactly wfB's task.
	row, err = q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		Partitions: []int32{partB},
	})
	if err != nil {
		t.Fatalf("dequeue restricted to partition B: %v", err)
	}
	if row.WorkflowID != wfB {
		t.Fatalf("partition-B poller got workflow_id %q, want %q", row.WorkflowID, wfB)
	}
}

// TestPartitionForMatchesStoreComputedShardID confirms StartWorkflowExecution
// actually stores the shard_id history.PartitionFor would compute for that
// workflow_id — proving shard_id is a real, correctly-computed value now,
// not just the inert 0 it defaulted to before this feature (Phase 2
// roadmap, Track C, item 3).
func TestPartitionForMatchesStoreComputedShardID(t *testing.T) {
	ctx := context.Background()
	pool, err := postgrespkg.NewPool(ctx, databaseDSN())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	const numShards = 8
	store := history.NewStore(pool, history.StoreOptions{NumShards: numShards})
	workflowID := fmt.Sprintf("shard-test-workflow-%d", time.Now().UnixNano())

	runID, err := store.StartWorkflowExecution(ctx, history.StartWorkflowExecutionParams{
		Namespace: "default", WorkflowID: workflowID, WorkflowType: "ShardTest",
		TaskQueue: "shard-test-queue", RequestID: workflowID + "-start",
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	q := sqlc.New(pool)
	ns, err := q.GetNamespaceByName(ctx, "default")
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: ns.ID, WorkflowID: workflowID, RunID: runID})
	if err != nil {
		t.Fatalf("get workflow_execution: %v", err)
	}

	want := history.PartitionFor(workflowID, numShards)
	if we.ShardID != want {
		t.Fatalf("stored shard_id = %d, want %d (history.PartitionFor(%q, %d))", we.ShardID, want, workflowID, numShards)
	}
}
