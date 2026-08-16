package history

import (
	"context"
	"fmt"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

type RequestCancelWorkflowExecutionParams struct {
	Namespace  string
	WorkflowID string
	RunID      string // empty means "current run"
	Reason     string
}

// RequestCancelWorkflowExecution appends WorkflowExecutionCancelRequested
// and WorkflowTaskScheduled, then enqueues a workflow task to wake the
// workflow — the exact same "out-of-band write, then a normal task" shape
// as SignalWorkflowExecution (Phase 2 roadmap, Track B, item 3). This does
// not itself close the run: the woken workflow task's code observes the
// request (via workflow.IsCancelRequested) and decides how to end —
// TerminateWorkflowExecution remains the separate, unconditional hard stop
// for when that cooperation can't be relied on.
func (s *Store) RequestCancelWorkflowExecution(ctx context.Context, p RequestCancelWorkflowExecutionParams) error {
	nsID, err := s.getNamespaceID(ctx, p.Namespace)
	if err != nil {
		return err
	}

	var taskQueue string
	err = s.withTxRetryOnConflict(ctx, func(q *sqlc.Queries) error {
		runID := p.RunID
		if runID == "" {
			cur, err := q.GetCurrentExecution(ctx, sqlc.GetCurrentExecutionParams{NamespaceID: nsID, WorkflowID: p.WorkflowID})
			if err != nil {
				return fmt.Errorf("get current_execution: %w", err)
			}
			runID = cur.RunID
		}

		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: p.WorkflowID, RunID: runID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}

		_, ids, err := AppendHistory(ctx, q, nsID, p.WorkflowID, runID, we.NextEventID, []NewEvent{
			{
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
				Attributes: &flowv1.WorkflowExecutionCancelRequestedEventAttributes{Reason: p.Reason},
			},
			{
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &flowv1.WorkflowTaskScheduledEventAttributes{TaskQueue: we.TaskQueue},
			},
		})
		if err != nil {
			return err
		}

		taskQueue = we.TaskQueue
		preferredWorker, stickyDeadline := stickyEnqueueFields(we)
		return q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
			NamespaceID: nsID, TaskQueueName: we.TaskQueue,
			TaskQueuePartition: PartitionFor(p.WorkflowID, s.numTaskQueuePartitions),
			WorkflowID:         p.WorkflowID, RunID: runID, ScheduledEventID: ids[1],
			PreferredWorkerIdentity: preferredWorker, StickyDeadline: stickyDeadline,
		})
	})
	if err != nil {
		return err
	}
	s.notifier.notify(workflowQueueKey(nsID, taskQueue))
	return nil
}
