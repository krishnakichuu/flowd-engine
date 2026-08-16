package history

import (
	"context"
	"encoding/binary"
	"fmt"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// defaultHistoryPageSize is used when the caller does not specify a page
// size for GetWorkflowExecutionHistory.
const defaultHistoryPageSize = 1000

// GetHistory returns one page of a workflow run's history, in order.
// next_page_token is the big-endian encoding of the last event_id returned,
// or nil when there is no further page.
func (s *Store) GetHistory(ctx context.Context, namespace, workflowID, runID string, pageSize int32, pageToken []byte) ([]*flowv1.HistoryEvent, []byte, error) {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return nil, nil, err
	}
	if pageSize <= 0 {
		pageSize = defaultHistoryPageSize
	}
	var after int64
	if len(pageToken) == 8 {
		// #nosec G115 -- event_id is a Postgres BIGINT assigned by our own
		// monotonic counter; it will not exceed int64 range in practice.
		after = int64(binary.BigEndian.Uint64(pageToken))
	}

	q := sqlc.New(s.pool)
	rows, err := q.ListHistoryEvents(ctx, sqlc.ListHistoryEventsParams{
		NamespaceID: nsID, WorkflowID: workflowID, RunID: runID,
		AfterEventID: after, PageLimit: pageSize,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("history: list history: %w", err)
	}

	events := make([]*flowv1.HistoryEvent, 0, len(rows))
	for _, r := range rows {
		ev, err := decodeHistoryEvent(r)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, ev)
	}

	var nextToken []byte
	// #nosec G115 -- pageSize bounds len(rows) well within int32 range
	// (defaultHistoryPageSize is 1000, and callers pass small values).
	if int32(len(rows)) == pageSize {
		nextToken = make([]byte, 8)
		// #nosec G115 -- event_id is never negative (assigned by our own
		// monotonic counter starting at 1).
		binary.BigEndian.PutUint64(nextToken, uint64(events[len(events)-1].EventId))
	}
	return events, nextToken, nil
}

type WorkflowExecutionSummary struct {
	Execution    flowv1.WorkflowExecution
	WorkflowType string
	TaskQueue    string
	Status       flowv1.WorkflowExecutionStatus
}

func (s *Store) DescribeWorkflowExecution(ctx context.Context, namespace, workflowID, runID string) (*WorkflowExecutionSummary, error) {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return nil, err
	}
	q := sqlc.New(s.pool)
	if runID == "" {
		cur, err := q.GetCurrentExecution(ctx, sqlc.GetCurrentExecutionParams{NamespaceID: nsID, WorkflowID: workflowID})
		if err != nil {
			return nil, fmt.Errorf("history: get current_execution: %w", err)
		}
		runID = cur.RunID
	}
	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: workflowID, RunID: runID})
	if err != nil {
		return nil, fmt.Errorf("history: get workflow_execution: %w", err)
	}
	return &WorkflowExecutionSummary{
		Execution:    flowv1.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		WorkflowType: we.WorkflowType,
		TaskQueue:    we.TaskQueue,
		Status:       statusFromString(we.Status),
	}, nil
}

// TerminateWorkflowExecution force-closes a run, appending
// WorkflowExecutionTerminated. It does not attempt to cancel any
// outstanding task lease — a terminated workflow simply stops being
// dispatched further work once its status is no longer RUNNING.
func (s *Store) TerminateWorkflowExecution(ctx context.Context, namespace, workflowID, runID, reason string) error {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return err
	}
	return s.withTx(ctx, func(q *sqlc.Queries) error {
		if runID == "" {
			cur, err := q.GetCurrentExecution(ctx, sqlc.GetCurrentExecutionParams{NamespaceID: nsID, WorkflowID: workflowID})
			if err != nil {
				return fmt.Errorf("get current_execution: %w", err)
			}
			runID = cur.RunID
		}
		we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: workflowID, RunID: runID})
		if err != nil {
			return fmt.Errorf("get workflow_execution: %w", err)
		}
		if _, _, err := AppendHistory(ctx, q, nsID, workflowID, runID, we.NextEventID, []NewEvent{
			{EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, Attributes: &flowv1.WorkflowExecutionTerminatedEventAttributes{Reason: reason}},
		}); err != nil {
			return err
		}
		return q.CloseWorkflowExecution(ctx, sqlc.CloseWorkflowExecutionParams{NamespaceID: nsID, WorkflowID: workflowID, RunID: runID, Status: "TERMINATED"})
	})
}

func statusFromString(s string) flowv1.WorkflowExecutionStatus {
	switch s {
	case "RUNNING":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_RUNNING
	case "COMPLETED":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED
	case "FAILED":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_FAILED
	case "TERMINATED":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TERMINATED
	case "TIMED_OUT":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TIMED_OUT
	case "CONTINUED_AS_NEW":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	default:
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
	}
}
