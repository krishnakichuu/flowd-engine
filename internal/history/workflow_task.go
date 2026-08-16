package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/metrics"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/proto"
)

// maxHistoryFetch bounds a single history read. Dispatch always sends the
// full history over the wire — even to a sticky worker resuming a cached
// Execution (see sdk/worker's cache) — since only the requesting worker
// reliably knows how much of it it's already processed; that worker slices
// off the tail itself. So dispatch reads up to this many events in one
// call rather than paginating internally.
const maxHistoryFetch = 100_000

// StickyExecutionAttributes is what a worker's RespondWorkflowTaskCompleted
// call attaches when it wants this run's next workflow task preferentially
// routed back to it, so it can resume its cached in-memory Execution
// instead of a full replay (Phase 2 roadmap, Track C, item 1). A nil value
// (or WorkerIdentity == "") clears any existing registration.
type StickyExecutionAttributes struct {
	WorkerIdentity         string
	ScheduleToStartTimeout time.Duration
}

type DispatchedWorkflowTask struct {
	TaskToken              []byte
	Execution              flowv1.WorkflowExecution
	WorkflowType           string
	History                []*flowv1.HistoryEvent
	PreviousStartedEventID int64
	// Query is set instead of this being a real workflow task dispatch —
	// see DispatchWorkflowTask's query-priority check.
	Query *DispatchedQuery
}

// DispatchWorkflowTask dequeues one pending workflow task (ADR-0002,
// Mechanism A: FOR UPDATE SKIP LOCKED) and, on success, separately appends a
// WorkflowTaskStarted event. These two steps do not need to share a
// transaction: if the process crashes between them, the task row is left
// STARTED with a live lease and the background reaper reclaims it for
// redispatch — no history is lost or corrupted either way.
//
// workerIdentity is who's asking: a task with no sticky registration is
// visible to anyone, but a task some prior response asked to be routed
// back to a specific worker stays invisible to every other identity until
// its sticky deadline passes (see the migration's DequeueWorkflowTask
// query and StickyExecutionAttributes).
//
// A pending query on this queue takes priority over a normal workflow
// task: the client on the other end of QueryWorkflowExecution is
// synchronously blocked, and answering a query is cheap (see
// query_tasks.sql). Only once none is pending does this fall through to
// the normal dequeue below.
//
// partitions restricts dispatch to those task_queue_partition values —
// empty means any partition (see PartitionFor and Phase 2 roadmap, Track
// C, item 3). Queries aren't partition-filtered: they're low-volume and
// latency-sensitive enough that restricting which workers can see them
// isn't worth the complexity yet.
func (s *Store) DispatchWorkflowTask(ctx context.Context, namespace, taskQueue, workerIdentity string, partitions []int32) (*DispatchedWorkflowTask, error) {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return nil, err
	}

	q := sqlc.New(s.pool)

	qRow, err := q.DequeueQueryTask(ctx, sqlc.DequeueQueryTaskParams{
		TaskQueueName: taskQueue, NamespaceID: nsID, LeaseSeconds: queryTaskLeaseSeconds,
	})
	if err == nil {
		return s.buildQueryDispatch(ctx, q, nsID, qRow)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("history: dequeue query task: %w", err)
	}

	row, err := q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName:  taskQueue,
		NamespaceID:    nsID,
		LeaseSeconds:   workflowTaskLeaseSeconds,
		WorkerIdentity: pgtype.Text{String: workerIdentity, Valid: workerIdentity != ""},
		Partitions:     partitions,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoTaskAvailable
		}
		return nil, fmt.Errorf("history: dequeue workflow task: %w", err)
	}

	var startedEventID int64
	txErr := s.withTx(ctx, func(q *sqlc.Queries) error {
		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: row.WorkflowID, RunID: row.RunID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}
		_, ids, err := AppendHistory(ctx, q, nsID, row.WorkflowID, row.RunID, we.NextEventID, []NewEvent{
			{EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_STARTED, Attributes: &flowv1.WorkflowTaskStartedEventAttributes{}},
		})
		if err != nil {
			return err
		}
		startedEventID = ids[0]
		return nil
	})
	if txErr != nil {
		return nil, fmt.Errorf("history: record workflow task started: %w", txErr)
	}

	historyRows, err := q.ListHistoryEvents(ctx, sqlc.ListHistoryEventsParams{
		NamespaceID: nsID, WorkflowID: row.WorkflowID, RunID: row.RunID,
		AfterEventID: 0, PageLimit: maxHistoryFetch,
	})
	if err != nil {
		return nil, fmt.Errorf("history: list history: %w", err)
	}
	events := make([]*flowv1.HistoryEvent, 0, len(historyRows))
	for _, r := range historyRows {
		ev, err := decodeHistoryEvent(r)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}

	token := newTaskToken(TaskTokenKindWorkflow, nsID, row.WorkflowID, row.RunID, row.TaskID, uuidString(row.LeaseToken))
	tokenBytes, err := token.Encode(s.taskTokenSigningKey)
	if err != nil {
		return nil, err
	}

	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: row.WorkflowID, RunID: row.RunID})
	if err != nil {
		return nil, fmt.Errorf("history: get workflow_execution: %w", err)
	}

	metrics.WorkflowTaskDispatchedTotal.Inc()
	return &DispatchedWorkflowTask{
		TaskToken:              tokenBytes,
		Execution:              flowv1.WorkflowExecution{WorkflowId: row.WorkflowID, RunId: row.RunID},
		WorkflowType:           we.WorkflowType,
		History:                events,
		PreviousStartedEventID: startedEventID,
	}, nil
}

// buildQueryDispatch turns a dequeued query_tasks row into the same shape
// as a real workflow task dispatch — a worker needs the full history to
// rebuild (or reuse its cached) in-memory state before it can answer —
// except no WorkflowTaskStarted event is appended: a query never touches
// history_events.
func (s *Store) buildQueryDispatch(ctx context.Context, q *sqlc.Queries, nsID int64, qRow sqlc.QueryTask) (*DispatchedWorkflowTask, error) {
	historyRows, err := q.ListHistoryEvents(ctx, sqlc.ListHistoryEventsParams{
		NamespaceID: nsID, WorkflowID: qRow.WorkflowID, RunID: qRow.RunID,
		AfterEventID: 0, PageLimit: maxHistoryFetch,
	})
	if err != nil {
		return nil, fmt.Errorf("history: list history for query: %w", err)
	}
	events := make([]*flowv1.HistoryEvent, 0, len(historyRows))
	for _, r := range historyRows {
		ev, err := decodeHistoryEvent(r)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}

	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: qRow.WorkflowID, RunID: qRow.RunID})
	if err != nil {
		return nil, fmt.Errorf("history: get workflow_execution for query: %w", err)
	}

	var args flowv1.Payload
	if err := proto.Unmarshal(qRow.QueryArgs, &args); err != nil {
		return nil, fmt.Errorf("history: unmarshal query args: %w", err)
	}

	token := newTaskToken(TaskTokenKindQuery, nsID, qRow.WorkflowID, qRow.RunID, qRow.TaskID, uuidString(qRow.LeaseToken))
	tokenBytes, err := token.Encode(s.taskTokenSigningKey)
	if err != nil {
		return nil, err
	}

	return &DispatchedWorkflowTask{
		TaskToken:    tokenBytes,
		Execution:    flowv1.WorkflowExecution{WorkflowId: qRow.WorkflowID, RunId: qRow.RunID},
		WorkflowType: we.WorkflowType,
		History:      events,
		Query:        &DispatchedQuery{QueryType: qRow.QueryType, QueryArgs: &args},
	}, nil
}

// CompleteWorkflowTask processes the commands a worker emitted in response
// to a workflow task. Deleting the task row (Mechanism A) and appending the
// resulting history events (Mechanism B) happen in one transaction: unlike
// dispatch, losing this atomicity would silently drop a workflow's progress
// with no lease left to reap (see ADR-0002). When the commands include a
// ContinueAsNewWorkflowExecutionCommand, closing this run and starting the
// replacement run also happen inside that same transaction — either both
// sides of the continuation are visible, or neither is.
//
// sticky, if non-nil, registers (or — if nil, or a prior round's
// registration and this round doesn't renew it — clears) this run's
// preferred worker for its next workflow task, in the same transaction as
// everything else here.
func (s *Store) CompleteWorkflowTask(ctx context.Context, token TaskToken, commands []*flowv1.Command, sticky *StickyExecutionAttributes) error {
	if token.Kind != TaskTokenKindWorkflow {
		return fmt.Errorf("history: task token is not a workflow task token")
	}

	var activitiesEnqueued bool
	var activityTaskQueue string
	var continuedTaskQueue string
	txErr := s.withTx(ctx, func(q *sqlc.Queries) error {
		affected, err := q.CompleteWorkflowTask(ctx, sqlc.CompleteWorkflowTaskParams{
			TaskID:     token.TaskID,
			LeaseToken: pgUUIDFromString(token.LeaseToken),
		})
		if err != nil {
			return fmt.Errorf("complete workflow task row: %w", err)
		}
		if affected == 0 {
			return ErrTaskLeaseExpired
		}

		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}

		events := []NewEvent{
			{EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_COMPLETED, Attributes: &flowv1.WorkflowTaskCompletedEventAttributes{}},
		}

		var closeStatus string
		var nextActivityEnqueues []sqlc.EnqueueActivityTaskParams
		// activityEventIndex tracks, per pending enqueue, the index into
		// `events` of its ActivityTaskScheduled event, so we can stamp the
		// row's scheduled_event_id with the ID AppendHistory assigns below.
		var activityEventIndex []int
		var timerEventIndex []int
		var pendingTimers []*flowv1.StartTimerCommand
		var terminalCommands int
		var scheduleOrTimerCommands int
		var continueAsNew *flowv1.ContinueAsNewWorkflowExecutionCommand
		var newRunID string

		for _, cmd := range commands {
			switch c := cmd.Command.(type) {
			case *flowv1.Command_ScheduleActivityTask:
				scheduleOrTimerCommands++
				sched := c.ScheduleActivityTask
				events = append(events, NewEvent{
					EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
					Attributes: &flowv1.ActivityTaskScheduledEventAttributes{
						ActivityId:             sched.ActivityId,
						ActivityType:           sched.ActivityType,
						Input:                  sched.Input,
						RetryPolicy:            sched.RetryPolicy,
						ScheduleToStartTimeout: sched.ScheduleToStartTimeout,
						StartToCloseTimeout:    sched.StartToCloseTimeout,
					},
				})
				activityEventIndex = append(activityEventIndex, len(events)-1)
				nextActivityEnqueues = append(nextActivityEnqueues, activityEnqueueParams(
					token.NamespaceID, we.TaskQueue, PartitionFor(token.WorkflowID, s.numTaskQueuePartitions),
					token.WorkflowID, token.RunID, sched,
				))
			case *flowv1.Command_StartTimer:
				scheduleOrTimerCommands++
				st := c.StartTimer
				events = append(events, NewEvent{
					EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_TIMER_STARTED,
					Attributes: &flowv1.TimerStartedEventAttributes{TimerId: st.TimerId, Duration: st.Duration},
				})
				timerEventIndex = append(timerEventIndex, len(events)-1)
				pendingTimers = append(pendingTimers, st)
			case *flowv1.Command_CompleteWorkflowExecution:
				terminalCommands++
				events = append(events, NewEvent{
					EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
					Attributes: &flowv1.WorkflowExecutionCompletedEventAttributes{Result: c.CompleteWorkflowExecution.Result},
				})
				closeStatus = "COMPLETED"
			case *flowv1.Command_FailWorkflowExecution:
				terminalCommands++
				events = append(events, NewEvent{
					EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
					Attributes: &flowv1.WorkflowExecutionFailedEventAttributes{Failure: c.FailWorkflowExecution.Failure},
				})
				closeStatus = "FAILED"
			case *flowv1.Command_ContinueAsNewWorkflowExecution:
				terminalCommands++
				continueAsNew = c.ContinueAsNewWorkflowExecution
				newRunID = uuid.NewString()
				events = append(events, NewEvent{
					EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
					Attributes: &flowv1.WorkflowExecutionContinuedAsNewEventAttributes{
						NewRunId:     newRunID,
						WorkflowType: stringOrFallback(continueAsNew.WorkflowType, we.WorkflowType),
						TaskQueue:    stringOrFallback(continueAsNew.TaskQueue, we.TaskQueue),
						Input:        continueAsNew.Input,
					},
				})
				closeStatus = "CONTINUED_AS_NEW"
			default:
				return fmt.Errorf("history: unsupported command %T", cmd.Command)
			}
		}

		if terminalCommands > 1 {
			return fmt.Errorf("history: workflow task response has %d terminal commands (complete/fail/continue-as-new), expected at most 1", terminalCommands)
		}
		if continueAsNew != nil && scheduleOrTimerCommands > 0 {
			return fmt.Errorf("history: continue-as-new cannot be combined with schedule-activity or start-timer commands in the same response")
		}

		_, assignedIDs, err := AppendHistory(ctx, q, token.NamespaceID, token.WorkflowID, token.RunID, we.NextEventID, events)
		if err != nil {
			return err
		}

		for i, params := range nextActivityEnqueues {
			params.ScheduledEventID = assignedIDs[activityEventIndex[i]]
			if err := q.EnqueueActivityTask(ctx, params); err != nil {
				return fmt.Errorf("enqueue activity task: %w", err)
			}
			activitiesEnqueued = true
			activityTaskQueue = we.TaskQueue
		}

		for i, st := range pendingTimers {
			if err := q.InsertTimer(ctx, sqlc.InsertTimerParams{
				NamespaceID:      token.NamespaceID,
				WorkflowID:       token.WorkflowID,
				RunID:            token.RunID,
				TimerID:          st.TimerId,
				FireAt:           pgtype.Timestamptz{Time: time.Now().Add(st.Duration.AsDuration()), Valid: true},
				ScheduledEventID: assignedIDs[timerEventIndex[i]],
			}); err != nil {
				return fmt.Errorf("insert timer: %w", err)
			}
		}

		if closeStatus != "" {
			if err := q.CloseWorkflowExecution(ctx, sqlc.CloseWorkflowExecutionParams{
				NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID, Status: closeStatus,
			}); err != nil {
				return fmt.Errorf("close workflow_execution: %w", err)
			}
			metrics.WorkflowCompletedTotal.WithLabelValues(closeStatus).Inc()
		}

		if continueAsNew != nil {
			if err := continueWorkflowAsNew(ctx, q, token, we, newRunID, continueAsNew, s.numShards, s.numTaskQueuePartitions); err != nil {
				return err
			}
			continuedTaskQueue = stringOrFallback(continueAsNew.TaskQueue, we.TaskQueue)
		}

		// A closed run (closeStatus != "") will never receive another
		// workflow task, so there's nothing to register stickiness for —
		// its cache entry, if any, is the worker's own business to expire.
		// An open run always gets this round's decision written, even to
		// clear a prior round's registration: a worker that stayed sticky
		// last time but declines this time (sticky == nil) must not leave
		// a stale preference pointing at it.
		if closeStatus == "" {
			var workerIdentity pgtype.Text
			var expiresAt pgtype.Timestamptz
			if sticky != nil && sticky.WorkerIdentity != "" {
				workerIdentity = pgtype.Text{String: sticky.WorkerIdentity, Valid: true}
				expiresAt = pgtype.Timestamptz{Time: time.Now().Add(sticky.ScheduleToStartTimeout), Valid: true}
			}
			if err := q.SetStickyExecution(ctx, sqlc.SetStickyExecutionParams{
				NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID,
				StickyWorkerIdentity: workerIdentity, StickyExpiresAt: expiresAt,
			}); err != nil {
				return fmt.Errorf("set sticky execution: %w", err)
			}
		}
		return nil
	})
	if txErr != nil {
		return txErr
	}
	// Timers aren't sync-matched here: TIMER_STARTED just schedules
	// RunTimerFirer's own ticker to pick it up later (timers.go), which
	// fires its own notify once the timer is actually due — notifying the
	// workflow queue now would just cause an immediate, wasted retry.
	if activitiesEnqueued {
		s.notifier.notify(activityQueueKey(token.NamespaceID, activityTaskQueue))
	}
	if continuedTaskQueue != "" {
		s.notifier.notify(workflowQueueKey(token.NamespaceID, continuedTaskQueue))
	}
	return nil
}

// continueWorkflowAsNew starts the replacement run a ContinueAsNew command
// asked for: current_executions is repointed at newRunID, a fresh
// workflow_executions row is created (status RUNNING, next_event_id reset
// to 1 — exactly what StartWorkflowExecution does for a brand new
// workflow_id), and its own WorkflowExecutionStarted/WorkflowTaskScheduled
// pair is appended so the first poll for newRunID looks, to the replayer,
// like any other fresh execution. we is the closing run's already-fetched
// row, used to resolve every field the command left unset.
func continueWorkflowAsNew(ctx context.Context, q *sqlc.Queries, token TaskToken, we sqlc.WorkflowExecution, newRunID string, cmd *flowv1.ContinueAsNewWorkflowExecutionCommand, numShards, numTaskQueuePartitions int32) error {
	newWorkflowType := stringOrFallback(cmd.WorkflowType, we.WorkflowType)
	newTaskQueue := stringOrFallback(cmd.TaskQueue, we.TaskQueue)

	// A synthetic request ID: unlike StartWorkflowExecution, continuation
	// isn't driven by a retryable client RPC (RespondWorkflowTaskCompleted's
	// own idempotency is the task lease, already checked above), so there's
	// no client-supplied value to de-duplicate against here.
	if err := q.ReplaceCurrentExecution(ctx, sqlc.ReplaceCurrentExecutionParams{
		NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID,
		RunID: newRunID, CreateRequestID: uuid.NewString(),
	}); err != nil {
		return fmt.Errorf("continue as new: replace current_execution: %w", err)
	}

	if err := q.CreateWorkflowExecution(ctx, sqlc.CreateWorkflowExecutionParams{
		NamespaceID:  token.NamespaceID,
		WorkflowID:   token.WorkflowID,
		RunID:        newRunID,
		WorkflowType: newWorkflowType,
		TaskQueue:    newTaskQueue,
		ShardID:      PartitionFor(token.WorkflowID, numShards),
		// workflow_execution_timeout spans the whole continue-as-new chain
		// in Phase 1 — carried forward unconditionally, not overridable by
		// the command (see ContinueAsNewWorkflowExecutionCommand's doc).
		WorkflowExecutionTimeoutNs: we.WorkflowExecutionTimeoutNs,
		WorkflowRunTimeoutNs:       durationOrFallback(cmd.WorkflowRunTimeout, we.WorkflowRunTimeoutNs),
		WorkflowTaskTimeoutNs:      durationOrFallback(cmd.WorkflowTaskTimeout, we.WorkflowTaskTimeoutNs),
	}); err != nil {
		return fmt.Errorf("continue as new: create workflow_execution: %w", err)
	}

	if _, _, err := AppendHistory(ctx, q, token.NamespaceID, token.WorkflowID, newRunID, 1, []NewEvent{
		{
			EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &flowv1.WorkflowExecutionStartedEventAttributes{
				WorkflowType:       newWorkflowType,
				TaskQueue:          newTaskQueue,
				Input:              cmd.Input,
				RetryPolicy:        cmd.RetryPolicy,
				ContinuedFromRunId: token.RunID,
			},
		},
		{
			EventType:  flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &flowv1.WorkflowTaskScheduledEventAttributes{TaskQueue: newTaskQueue},
		},
	}); err != nil {
		return fmt.Errorf("continue as new: append start events: %w", err)
	}

	// Same as StartWorkflowExecution: a brand new run has no worker cache
	// to be sticky to yet — no preference set.
	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID:        token.NamespaceID,
		TaskQueueName:      newTaskQueue,
		TaskQueuePartition: PartitionFor(token.WorkflowID, numTaskQueuePartitions),
		WorkflowID:         token.WorkflowID,
		RunID:              newRunID,
		ScheduledEventID:   2,
	}); err != nil {
		return fmt.Errorf("continue as new: enqueue workflow task: %w", err)
	}

	metrics.WorkflowStartedTotal.Inc()
	return nil
}

// FailWorkflowTask handles a worker reporting it could not process a task
// (panic or detected non-determinism). The task lease is released back to
// PENDING immediately for redispatch; per ADR-0002's compact-history
// tradeoff this is not itself recorded as a history event, only via
// metrics.WorkflowTaskFailedTotal (mirrors how per-attempt activity
// failures are handled by metrics.ActivityRetryTotal) — unlike an activity
// failure, there's no bounded retry count here: a workflow whose code
// panics deterministically will fail, redispatch, and fail again every
// time it's replayed, so this metric is the only thing that makes that
// loop visible instead of just quietly consuming CPU forever.
func (s *Store) FailWorkflowTask(ctx context.Context, token TaskToken, _ *flowv1.Failure) error {
	if token.Kind != TaskTokenKindWorkflow {
		return fmt.Errorf("history: task token is not a workflow task token")
	}
	metrics.WorkflowTaskFailedTotal.Inc()
	q := sqlc.New(s.pool)
	affected, err := q.CompleteWorkflowTask(ctx, sqlc.CompleteWorkflowTaskParams{TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken)})
	if err != nil {
		return fmt.Errorf("history: release failed workflow task: %w", err)
	}
	if affected == 0 {
		return ErrTaskLeaseExpired
	}
	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID})
	if err != nil {
		return fmt.Errorf("history: get workflow_execution: %w", err)
	}
	// Clear any sticky registration rather than reuse it: this worker just
	// failed processing this run (panic or non-determinism), so its cached
	// Execution — if any — is exactly what's suspect. Redispatch goes to
	// whichever worker is available, forcing a full replay.
	if err := q.SetStickyExecution(ctx, sqlc.SetStickyExecutionParams{
		NamespaceID: token.NamespaceID, WorkflowID: token.WorkflowID, RunID: token.RunID,
	}); err != nil {
		return fmt.Errorf("history: clear sticky execution: %w", err)
	}
	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: token.NamespaceID, TaskQueueName: we.TaskQueue,
		TaskQueuePartition: PartitionFor(token.WorkflowID, s.numTaskQueuePartitions),
		WorkflowID:         token.WorkflowID, RunID: token.RunID, ScheduledEventID: we.NextEventID - 1,
	}); err != nil {
		return err
	}
	s.notifier.notify(workflowQueueKey(token.NamespaceID, we.TaskQueue))
	return nil
}

func uuidString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	return uuid.UUID(u.Bytes).String()
}
