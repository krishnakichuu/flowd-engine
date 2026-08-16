package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/metrics"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

type StartWorkflowExecutionParams struct {
	Namespace                string
	WorkflowID               string
	WorkflowType             string
	TaskQueue                string
	Input                    *flowv1.Payload
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	RetryPolicy              *flowv1.RetryPolicy
	RequestID                string
}

// errIdempotentReplay is an internal sentinel used to short-circuit
// withTx's error return into a non-error, successful "same run as before"
// response for StartWorkflowExecution's idempotency handling.
var errIdempotentReplay = errors.New("history: idempotent replay")

// StartWorkflowExecution creates a new run, atomically appending
// WorkflowExecutionStarted and WorkflowTaskScheduled as events 1 and 2 and
// enqueuing the first workflow task, all in one transaction. Idempotent on
// (namespace, workflow_id, request_id): a retried call with the same
// request_id returns the existing run_id rather than erroring or starting a
// duplicate run.
func (s *Store) StartWorkflowExecution(ctx context.Context, p StartWorkflowExecutionParams) (runID string, err error) {
	nsID, err := s.getNamespaceID(ctx, p.Namespace)
	if err != nil {
		return "", err
	}

	newRunID := uuid.NewString()

	txErr := s.withTx(ctx, func(q *sqlc.Queries) error {
		inserted, err := q.InsertCurrentExecution(ctx, sqlc.InsertCurrentExecutionParams{
			NamespaceID:     nsID,
			WorkflowID:      p.WorkflowID,
			RunID:           newRunID,
			CreateRequestID: p.RequestID,
		})
		if err != nil {
			return fmt.Errorf("insert current_execution: %w", err)
		}

		if inserted == 0 {
			existing, err := q.GetCurrentExecution(ctx, sqlc.GetCurrentExecutionParams{NamespaceID: nsID, WorkflowID: p.WorkflowID})
			if err != nil {
				return fmt.Errorf("get current_execution: %w", err)
			}
			if existing.CreateRequestID == p.RequestID {
				newRunID = existing.RunID
				return errIdempotentReplay
			}

			we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: p.WorkflowID, RunID: existing.RunID})
			if err != nil {
				return fmt.Errorf("get workflow_execution: %w", err)
			}
			if we.Status == "RUNNING" {
				return ErrWorkflowAlreadyRunning
			}

			if err := q.ReplaceCurrentExecution(ctx, sqlc.ReplaceCurrentExecutionParams{
				NamespaceID: nsID, WorkflowID: p.WorkflowID, RunID: newRunID, CreateRequestID: p.RequestID,
			}); err != nil {
				return fmt.Errorf("replace current_execution: %w", err)
			}
		}

		if err := q.CreateWorkflowExecution(ctx, sqlc.CreateWorkflowExecutionParams{
			NamespaceID:                nsID,
			WorkflowID:                 p.WorkflowID,
			RunID:                      newRunID,
			WorkflowType:               p.WorkflowType,
			TaskQueue:                  p.TaskQueue,
			ShardID:                    PartitionFor(p.WorkflowID, s.numShards),
			WorkflowExecutionTimeoutNs: durationToPgInt8(p.WorkflowExecutionTimeout),
			WorkflowRunTimeoutNs:       durationToPgInt8(p.WorkflowRunTimeout),
			WorkflowTaskTimeoutNs:      durationToPgInt8(p.WorkflowTaskTimeout),
		}); err != nil {
			return fmt.Errorf("create workflow_execution: %w", err)
		}

		_, _, err = AppendHistory(ctx, q, nsID, p.WorkflowID, newRunID, 1, []NewEvent{
			{
				EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &flowv1.WorkflowExecutionStartedEventAttributes{
					WorkflowType: p.WorkflowType,
					TaskQueue:    p.TaskQueue,
					Input:        p.Input,
					RetryPolicy:  p.RetryPolicy,
				},
			},
			{
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &flowv1.WorkflowTaskScheduledEventAttributes{TaskQueue: p.TaskQueue},
			},
		})
		if err != nil {
			return fmt.Errorf("append start events: %w", err)
		}

		// A brand new run has no worker cache to be sticky to yet —
		// PreferredWorkerIdentity/StickyDeadline are left at their zero
		// value (no preference), same as before sticky caching existed.
		if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
			NamespaceID:        nsID,
			TaskQueueName:      p.TaskQueue,
			TaskQueuePartition: PartitionFor(p.WorkflowID, s.numTaskQueuePartitions),
			WorkflowID:         p.WorkflowID,
			RunID:              newRunID,
			ScheduledEventID:   2,
		}); err != nil {
			return fmt.Errorf("enqueue workflow task: %w", err)
		}
		return nil
	})

	if txErr != nil && !errors.Is(txErr, errIdempotentReplay) {
		return "", txErr
	}
	if txErr == nil {
		metrics.WorkflowStartedTotal.Inc()
		s.notifier.notify(workflowQueueKey(nsID, p.TaskQueue))
	}
	return newRunID, nil
}

func durationToPgInt8(d time.Duration) pgtype.Int8 {
	if d <= 0 {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: int64(d), Valid: true}
}
