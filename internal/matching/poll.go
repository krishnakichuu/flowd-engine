// Package matching implements the long-poll contract PollWorkflowTaskQueue
// and PollActivityTaskQueue promise to workers, plus the background lease
// reaper (reaper.go). The underlying dequeue SQL (FOR UPDATE SKIP LOCKED,
// ADR-0002 Mechanism A) lives on internal/history.Store, since a
// dispatched task's lease and its recorded Started event are managed
// together there; this package adds the retry-until-available-or-deadline
// polling behavior on top. Between dequeue attempts, that retry no longer
// sleeps blindly — it waits on the Store's in-memory notifier (see
// history.Store.WaitForWorkflowTask/WaitForActivityTask), which wakes it
// immediately when this process enqueues a matching task ("sync match",
// Phase 2 roadmap, Track C, item 2), falling back to the old fixed
// interval only when nothing notifies it in time.
package matching

import (
	"context"
	"errors"

	"github.com/krishnakichuu/flowd/internal/history"
)

// PollWorkflowTask retries Store.DispatchWorkflowTask until a task is
// available or ctx is done (the frontend sets ctx's deadline to the
// long-poll timeout, default ~60s). workerIdentity is forwarded to
// DispatchWorkflowTask for sticky routing (see StickyExecutionAttributes).
// partitions restricts dispatch to those task_queue_partition values —
// empty means any partition (Phase 2 roadmap, Track C, item 3).
func PollWorkflowTask(ctx context.Context, store *history.Store, namespace, taskQueue, workerIdentity string, partitions []int32) (*history.DispatchedWorkflowTask, error) {
	for {
		task, err := store.DispatchWorkflowTask(ctx, namespace, taskQueue, workerIdentity, partitions)
		if err == nil {
			return task, nil
		}
		if !errors.Is(err, history.ErrNoTaskAvailable) {
			return nil, err
		}
		if err := store.WaitForWorkflowTask(ctx, namespace, taskQueue); err != nil {
			return nil, err
		}
	}
}

// PollActivityTask is PollWorkflowTask's activity-queue counterpart.
func PollActivityTask(ctx context.Context, store *history.Store, namespace, taskQueue string, partitions []int32) (*history.DispatchedActivityTask, error) {
	for {
		task, err := store.DispatchActivityTask(ctx, namespace, taskQueue, partitions)
		if err == nil {
			return task, nil
		}
		if !errors.Is(err, history.ErrNoTaskAvailable) {
			return nil, err
		}
		if err := store.WaitForActivityTask(ctx, namespace, taskQueue); err != nil {
			return nil, err
		}
	}
}
