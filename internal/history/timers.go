package history

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// RunTimerFirer scans for due timers (workflow.Sleep/NewTimer) on a fixed
// interval and, for each, appends TimerFired + WorkflowTaskScheduled and
// enqueues a workflow task — the same reaper-style background pattern as
// the task-lease reaper, but producing history events, which is why it
// lives in the history package rather than internal/matching.
func (s *Store) RunTimerFirer(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for s.fireDueTimer(ctx, logger) {
			}
		}
	}
}

// fireDueTimer fires at most one due timer and reports whether it did, so
// the caller can drain all currently-due timers before waiting for the next
// tick.
func (s *Store) fireDueTimer(ctx context.Context, logger *slog.Logger) bool {
	fired := false
	var nsID int64
	var taskQueue string
	err := s.withTx(ctx, func(q *sqlc.Queries) error {
		t, err := q.DequeueDueTimer(ctx)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}

		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{
			NamespaceID: t.NamespaceID, WorkflowID: t.WorkflowID, RunID: t.RunID,
		})
		if err != nil {
			return err
		}

		_, ids, err := AppendHistory(ctx, q, t.NamespaceID, t.WorkflowID, t.RunID, we.NextEventID, []NewEvent{
			{EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_TIMER_FIRED, Attributes: &flowv1.TimerFiredEventAttributes{TimerId: t.TimerID}},
			{EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, Attributes: &flowv1.WorkflowTaskScheduledEventAttributes{TaskQueue: we.TaskQueue}},
		})
		if err != nil {
			return err
		}

		preferredWorker, stickyDeadline := stickyEnqueueFields(we)
		if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
			NamespaceID: t.NamespaceID, TaskQueueName: we.TaskQueue,
			TaskQueuePartition: PartitionFor(t.WorkflowID, s.numTaskQueuePartitions),
			WorkflowID:         t.WorkflowID, RunID: t.RunID, ScheduledEventID: ids[1],
			PreferredWorkerIdentity: preferredWorker, StickyDeadline: stickyDeadline,
		}); err != nil {
			return err
		}
		fired = true
		nsID, taskQueue = t.NamespaceID, we.TaskQueue
		return nil
	})
	if err != nil {
		logger.Error("timer firer: failed to fire due timer", "error", err)
		return false
	}
	if fired {
		s.notifier.notify(workflowQueueKey(nsID, taskQueue))
	}
	return fired
}
