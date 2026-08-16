package history

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/metrics"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/proto"
)

type DispatchedActivityTask struct {
	TaskToken           []byte
	Execution           flowv1.WorkflowExecution
	ActivityID          int64
	ActivityType        string
	Input               *flowv1.Payload
	CurrentAttempt      int32
	StartToCloseTimeout time.Duration
}

// DispatchActivityTask dequeues one pending activity task (ADR-0002,
// Mechanism A) and separately appends an ActivityTaskStarted event, same
// non-atomic-by-design reasoning as DispatchWorkflowTask. partitions
// restricts dispatch to those task_queue_partition values — empty means
// any partition (see PartitionFor and Phase 2 roadmap, Track C, item 3).
func (s *Store) DispatchActivityTask(ctx context.Context, namespace, taskQueue string, partitions []int32) (*DispatchedActivityTask, error) {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return nil, err
	}

	q := sqlc.New(s.pool)
	row, err := q.DequeueActivityTask(ctx, sqlc.DequeueActivityTaskParams{TaskQueueName: taskQueue, NamespaceID: nsID, Partitions: partitions})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoTaskAvailable
		}
		return nil, fmt.Errorf("history: dequeue activity task: %w", err)
	}

	txErr := s.withTx(ctx, func(q *sqlc.Queries) error {
		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: row.WorkflowID, RunID: row.RunID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}
		_, _, err = AppendHistory(ctx, q, nsID, row.WorkflowID, row.RunID, we.NextEventID, []NewEvent{
			{
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_STARTED,
				Attributes: &flowv1.ActivityTaskStartedEventAttributes{ActivityId: row.ActivityID, Attempt: row.Attempt},
			},
		})
		return err
	})
	if txErr != nil {
		return nil, fmt.Errorf("history: record activity task started: %w", txErr)
	}

	var input flowv1.Payload
	if err := proto.Unmarshal(row.Input, &input); err != nil {
		return nil, fmt.Errorf("history: unmarshal activity input: %w", err)
	}

	token := newTaskToken(TaskTokenKindActivity, nsID, row.WorkflowID, row.RunID, row.TaskID, uuidString(row.LeaseToken))
	tokenBytes, err := token.Encode(s.taskTokenSigningKey)
	if err != nil {
		return nil, err
	}

	metrics.ActivityTaskDispatchedTotal.Inc()
	return &DispatchedActivityTask{
		TaskToken:           tokenBytes,
		Execution:           flowv1.WorkflowExecution{WorkflowId: row.WorkflowID, RunId: row.RunID},
		ActivityID:          row.ActivityID,
		ActivityType:        row.ActivityType,
		Input:               &input,
		CurrentAttempt:      row.Attempt,
		StartToCloseTimeout: time.Duration(row.StartToCloseTimeoutNs),
	}, nil
}

// CompleteActivityTask records a successful activity result and wakes the
// workflow: it deletes the activity_tasks row, appends
// ActivityTaskCompleted + WorkflowTaskScheduled, and enqueues a new workflow
// task, all atomically (ADR-0002 — losing this atomicity would silently
// drop the result with no lease left to reap).
func (s *Store) CompleteActivityTask(ctx context.Context, token TaskToken, result *flowv1.Payload) error {
	if token.Kind != TaskTokenKindActivity {
		return fmt.Errorf("history: task token is not an activity task token")
	}
	var taskQueue string
	err := s.withTx(ctx, func(q *sqlc.Queries) error {
		row, err := s.completeActivityTaskRow(ctx, q, token)
		if err != nil {
			return err
		}

		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}

		_, ids, err := AppendHistory(ctx, q, token.NamespaceID, token.WorkflowID, token.RunID, we.NextEventID, []NewEvent{
			{
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
				Attributes: &flowv1.ActivityTaskCompletedEventAttributes{ActivityId: row.ActivityID, Result: result},
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
			NamespaceID: token.NamespaceID, TaskQueueName: we.TaskQueue,
			TaskQueuePartition: PartitionFor(token.WorkflowID, s.numTaskQueuePartitions),
			WorkflowID:         token.WorkflowID, RunID: token.RunID, ScheduledEventID: ids[1],
			PreferredWorkerIdentity: preferredWorker, StickyDeadline: stickyDeadline,
		})
	})
	if err != nil {
		return err
	}
	s.notifier.notify(workflowQueueKey(token.NamespaceID, taskQueue))
	return nil
}

// FailActivityTask enforces server-side retry/backoff (ADR-0002): if
// attempts remain and the error type is not in non_retryable_error_types,
// the task is re-queued with backoff and no history event is written per
// attempt (compact history). Otherwise the failure is terminal: it is
// appended to history and the workflow is woken, same shape as
// CompleteActivityTask.
func (s *Store) FailActivityTask(ctx context.Context, token TaskToken, failure *flowv1.Failure) error {
	if token.Kind != TaskTokenKindActivity {
		return fmt.Errorf("history: task token is not an activity task token")
	}

	q := sqlc.New(s.pool)
	row, err := q.GetActivityTaskByLease(ctx, sqlc.GetActivityTaskByLeaseParams{TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken)})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskLeaseExpired
		}
		return fmt.Errorf("history: get activity task: %w", err)
	}

	retryable := row.RetryMaxAttempts == 0 || row.Attempt < row.RetryMaxAttempts
	for _, nonRetryable := range row.RetryNonRetryableTypes {
		if nonRetryable == failure.Type {
			retryable = false
			break
		}
	}

	if retryable {
		backoff := time.Duration(row.RetryInitialIntervalNs) * time.Duration(math.Pow(row.RetryBackoffCoefficient, float64(row.Attempt-1)))
		if maxInterval := time.Duration(row.RetryMaxIntervalNs); maxInterval > 0 && backoff > maxInterval {
			backoff = maxInterval
		}
		affected, err := q.RetryActivityTask(ctx, sqlc.RetryActivityTaskParams{
			TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken), BackoffSeconds: backoff.Seconds(),
		})
		if err != nil {
			return fmt.Errorf("history: retry activity task: %w", err)
		}
		if affected == 0 {
			return ErrTaskLeaseExpired
		}
		metrics.ActivityRetryTotal.Inc()
		return nil
	}

	var taskQueue string
	txErr := s.withTx(ctx, func(q *sqlc.Queries) error {
		affected, err := q.CompleteActivityTask(ctx, sqlc.CompleteActivityTaskParams{TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken)})
		if err != nil {
			return fmt.Errorf("delete activity task row: %w", err)
		}
		if affected == 0 {
			return ErrTaskLeaseExpired
		}

		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}

		_, ids, err := AppendHistory(ctx, q, token.NamespaceID, token.WorkflowID, token.RunID, we.NextEventID, []NewEvent{
			{
				EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_FAILED,
				Attributes: &flowv1.ActivityTaskFailedEventAttributes{ActivityId: row.ActivityID, Failure: failure, Attempt: row.Attempt},
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
			NamespaceID: token.NamespaceID, TaskQueueName: we.TaskQueue,
			TaskQueuePartition: PartitionFor(token.WorkflowID, s.numTaskQueuePartitions),
			WorkflowID:         token.WorkflowID, RunID: token.RunID, ScheduledEventID: ids[1],
			PreferredWorkerIdentity: preferredWorker, StickyDeadline: stickyDeadline,
		})
	})
	if txErr != nil {
		return txErr
	}
	s.notifier.notify(workflowQueueKey(token.NamespaceID, taskQueue))
	return nil
}

func (s *Store) completeActivityTaskRow(ctx context.Context, q *sqlc.Queries, token TaskToken) (sqlc.ActivityTask, error) {
	row, err := q.GetActivityTaskByLease(ctx, sqlc.GetActivityTaskByLeaseParams{TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken)})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return sqlc.ActivityTask{}, ErrTaskLeaseExpired
		}
		return sqlc.ActivityTask{}, fmt.Errorf("get activity task: %w", err)
	}
	affected, err := q.CompleteActivityTask(ctx, sqlc.CompleteActivityTaskParams{TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken)})
	if err != nil {
		return sqlc.ActivityTask{}, fmt.Errorf("delete activity task row: %w", err)
	}
	if affected == 0 {
		return sqlc.ActivityTask{}, ErrTaskLeaseExpired
	}
	return row, nil
}
