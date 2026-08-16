package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/proto"
)

// queryTaskLeaseSeconds is short: a query task's whole point is a client
// synchronously waiting on QueryWorkflowExecution, so a crashed worker's
// lease needs to be reclaimed quickly, not on the same timescale as a
// workflow task's real work.
const queryTaskLeaseSeconds = 10

// DispatchedQuery is set on a DispatchedWorkflowTask when the dispatch is
// actually a pending query, not a real workflow task — see
// DispatchWorkflowTask's query-priority check.
type DispatchedQuery struct {
	QueryType string
	QueryArgs *flowv1.Payload
}

type EnqueueQueryParams struct {
	Namespace  string
	WorkflowID string
	RunID      string // empty means "current run"
	QueryType  string
	QueryArgs  *flowv1.Payload
}

// EnqueueQuery creates a pending query_tasks row on the run's task queue
// and returns enough (namespaceID, taskID) to poll GetQueryResult for the
// answer. It never appends to history_events — see the query_tasks
// migration's doc for why that's a separate table and lifecycle from
// workflow_tasks/activity_tasks.
func (s *Store) EnqueueQuery(ctx context.Context, p EnqueueQueryParams) (namespaceID, taskID int64, err error) {
	nsID, err := s.getNamespaceID(ctx, p.Namespace)
	if err != nil {
		return 0, 0, err
	}
	q := sqlc.New(s.pool)

	runID := p.RunID
	if runID == "" {
		cur, err := q.GetCurrentExecution(ctx, sqlc.GetCurrentExecutionParams{NamespaceID: nsID, WorkflowID: p.WorkflowID})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, 0, ErrWorkflowNotFound
			}
			return 0, 0, fmt.Errorf("history: get current_execution: %w", err)
		}
		runID = cur.RunID
	}

	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: p.WorkflowID, RunID: runID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, ErrWorkflowNotFound
		}
		return 0, 0, fmt.Errorf("history: get workflow_execution: %w", err)
	}

	argsBytes, err := proto.Marshal(p.QueryArgs)
	if err != nil {
		return 0, 0, fmt.Errorf("history: marshal query args: %w", err)
	}

	task, err := q.EnqueueQueryTask(ctx, sqlc.EnqueueQueryTaskParams{
		NamespaceID: nsID, TaskQueueName: we.TaskQueue, WorkflowID: p.WorkflowID, RunID: runID,
		QueryType: p.QueryType, QueryArgs: argsBytes,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("history: enqueue query task: %w", err)
	}
	return nsID, task.TaskID, nil
}

// QueryResult is one snapshot of a query_tasks row's outcome, as read by
// QueryWorkflowExecution's own polling loop.
type QueryResult struct {
	Status         string // PENDING, STARTED, COMPLETED, or FAILED
	Result         *flowv1.Payload
	FailureMessage string
}

// GetQueryResult reads a query task's current status/result.
func (s *Store) GetQueryResult(ctx context.Context, namespaceID, taskID int64) (QueryResult, error) {
	q := sqlc.New(s.pool)
	row, err := q.GetQueryTask(ctx, sqlc.GetQueryTaskParams{TaskID: taskID, NamespaceID: namespaceID})
	if err != nil {
		return QueryResult{}, fmt.Errorf("history: get query task: %w", err)
	}
	res := QueryResult{Status: row.Status, FailureMessage: row.FailureMessage.String}
	if len(row.Result) > 0 {
		var p flowv1.Payload
		if err := proto.Unmarshal(row.Result, &p); err != nil {
			return QueryResult{}, fmt.Errorf("history: unmarshal query result: %w", err)
		}
		res.Result = &p
	}
	return res, nil
}

// DeleteQueryTask removes a query_tasks row once its result has been
// consumed — these rows aren't self-deleting like workflow/activity tasks
// (see the migration's doc): the one reader is QueryWorkflowExecution's own
// handler, and it owns cleaning up after itself.
func (s *Store) DeleteQueryTask(ctx context.Context, taskID int64) error {
	q := sqlc.New(s.pool)
	if err := q.DeleteQueryTask(ctx, taskID); err != nil {
		return fmt.Errorf("history: delete query task: %w", err)
	}
	return nil
}

// CompleteQuery records a successful query answer. Unlike
// CompleteWorkflowTask/CompleteActivityTask, this never touches
// history_events.
func (s *Store) CompleteQuery(ctx context.Context, token TaskToken, result *flowv1.Payload) error {
	if token.Kind != TaskTokenKindQuery {
		return fmt.Errorf("history: task token is not a query task token")
	}
	resultBytes, err := proto.Marshal(result)
	if err != nil {
		return fmt.Errorf("history: marshal query result: %w", err)
	}
	q := sqlc.New(s.pool)
	affected, err := q.CompleteQueryTask(ctx, sqlc.CompleteQueryTaskParams{
		TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken), Result: resultBytes,
	})
	if err != nil {
		return fmt.Errorf("history: complete query task: %w", err)
	}
	if affected == 0 {
		return ErrTaskLeaseExpired
	}
	return nil
}

// FailQuery records a query that a worker couldn't answer (no handler
// registered for that query_type, or the handler itself returned an
// error).
func (s *Store) FailQuery(ctx context.Context, token TaskToken, message string) error {
	if token.Kind != TaskTokenKindQuery {
		return fmt.Errorf("history: task token is not a query task token")
	}
	q := sqlc.New(s.pool)
	affected, err := q.FailQueryTask(ctx, sqlc.FailQueryTaskParams{
		TaskID: token.TaskID, LeaseToken: pgUUIDFromString(token.LeaseToken),
		FailureMessage: pgtype.Text{String: message, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("history: fail query task: %w", err)
	}
	if affected == 0 {
		return ErrTaskLeaseExpired
	}
	return nil
}
