package history

import (
	"context"
	"fmt"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

type SignalWorkflowExecutionParams struct {
	Namespace  string
	WorkflowID string
	RunID      string // empty means "current run"
	SignalName string
	Input      *flowv1.Payload
}

// SignalWorkflowExecution appends WorkflowExecutionSignaled and
// WorkflowTaskScheduled, then enqueues a workflow task to wake the
// workflow. Implemented for real in Phase 1 (unlike Query, which is
// deferred) because it is cheap and demonstrates the event-sourcing model
// well — see plan Context. Retries via withTxRetryOnConflict: this is an
// out-of-band write against a run that may have an active workflow task
// completing at the same moment (found live while building
// RequestCancelWorkflowExecution, which shares this exact shape and hit
// the same race under a genuinely concurrent signal/cancel).
func (s *Store) SignalWorkflowExecution(ctx context.Context, p SignalWorkflowExecutionParams) error {
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
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &flowv1.WorkflowExecutionSignaledEventAttributes{SignalName: p.SignalName, Input: p.Input},
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
