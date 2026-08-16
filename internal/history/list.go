package history

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// defaultListWorkflowsPageSize is used when the caller does not specify a
// page size for ListWorkflowExecutions.
const defaultListWorkflowsPageSize = 50

// ListWorkflowExecutionsParams filters/paginates ListWorkflowExecutions.
// StatusFilter left at its zero value (UNSPECIFIED) matches every status;
// TaskQueue left empty matches every task queue.
type ListWorkflowExecutionsParams struct {
	Namespace    string
	StatusFilter flowv1.WorkflowExecutionStatus
	TaskQueue    string
	PageSize     int32
	PageToken    []byte
}

// listWorkflowsCursor is the JSON payload opaque page tokens encode —
// plain JSON rather than GetHistory's fixed-width binary encoding (see
// query.go) because a keyset cursor here needs three fields, not one
// counter.
type listWorkflowsCursor struct {
	StartTime  time.Time `json:"t"`
	WorkflowID string    `json:"w"`
	RunID      string    `json:"r"`
}

// ListWorkflowExecutions returns one page of workflow executions in a
// namespace, newest-started first (see idx_workflow_executions_by_start_time,
// migration 0004) — the primitive both flow-cli's `list` subcommand and the
// web UI (internal/webapi) use to discover workflow IDs, since every other
// workflow RPC requires the caller to already know one.
func (s *Store) ListWorkflowExecutions(ctx context.Context, p ListWorkflowExecutionsParams) ([]WorkflowExecutionInfo, []byte, error) {
	nsID, err := s.getNamespaceID(ctx, p.Namespace)
	if err != nil {
		return nil, nil, err
	}
	pageSize := p.PageSize
	if pageSize <= 0 {
		pageSize = defaultListWorkflowsPageSize
	}

	var cursor pgtype.Timestamptz
	var cursorWorkflowID, cursorRunID pgtype.Text
	if len(p.PageToken) > 0 {
		var c listWorkflowsCursor
		if err := json.Unmarshal(p.PageToken, &c); err != nil {
			return nil, nil, fmt.Errorf("history: invalid page token: %w", err)
		}
		cursor = pgtype.Timestamptz{Time: c.StartTime, Valid: true}
		cursorWorkflowID = pgtype.Text{String: c.WorkflowID, Valid: true}
		cursorRunID = pgtype.Text{String: c.RunID, Valid: true}
	}

	var statusFilter pgtype.Text
	if p.StatusFilter != flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		statusFilter = pgtype.Text{String: statusToString(p.StatusFilter), Valid: true}
	}
	var taskQueue pgtype.Text
	if p.TaskQueue != "" {
		taskQueue = pgtype.Text{String: p.TaskQueue, Valid: true}
	}

	q := sqlc.New(s.pool)
	rows, err := q.ListWorkflowExecutions(ctx, sqlc.ListWorkflowExecutionsParams{
		NamespaceID:      nsID,
		StatusFilter:     statusFilter,
		TaskQueue:        taskQueue,
		CursorStartTime:  cursor,
		CursorWorkflowID: cursorWorkflowID,
		CursorRunID:      cursorRunID,
		PageLimit:        pageSize,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("history: list workflow executions: %w", err)
	}

	infos := make([]WorkflowExecutionInfo, 0, len(rows))
	for _, r := range rows {
		info := WorkflowExecutionInfo{
			WorkflowID:   r.WorkflowID,
			RunID:        r.RunID,
			WorkflowType: r.WorkflowType,
			TaskQueue:    r.TaskQueue,
			Status:       statusFromString(r.Status),
			StartTime:    r.StartTime.Time,
		}
		if r.CloseTime.Valid {
			info.CloseTime = r.CloseTime.Time
		}
		infos = append(infos, info)
	}

	var nextToken []byte
	// #nosec G115 -- pageSize bounds len(rows) well within int32 range
	// (defaultListWorkflowsPageSize is 50, and callers pass small values).
	if int32(len(rows)) == pageSize {
		last := rows[len(rows)-1]
		nextToken, err = json.Marshal(listWorkflowsCursor{
			StartTime: last.StartTime.Time, WorkflowID: last.WorkflowID, RunID: last.RunID,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("history: encode page token: %w", err)
		}
	}
	return infos, nextToken, nil
}

// WorkflowExecutionInfo is one row of a ListWorkflowExecutions result —
// the internal/history-level shape internal/frontend converts to the
// proto WorkflowExecutionInfo message. WorkflowID/RunID are plain strings
// rather than an embedded flowv1.WorkflowExecution so values of this type
// can be freely copied (appended to a slice, returned by value) without
// copying that message's internal mutex (see protoimpl.MessageState) —
// ToProto builds the proto message fresh, once, at the end.
type WorkflowExecutionInfo struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	TaskQueue    string
	Status       flowv1.WorkflowExecutionStatus
	StartTime    time.Time
	// CloseTime is the zero time for a still-RUNNING execution.
	CloseTime time.Time
}

// ToProto converts a WorkflowExecutionInfo to its wire representation.
func (i WorkflowExecutionInfo) ToProto() *flowv1.WorkflowExecutionInfo {
	pb := &flowv1.WorkflowExecutionInfo{
		Execution:    &flowv1.WorkflowExecution{WorkflowId: i.WorkflowID, RunId: i.RunID},
		WorkflowType: i.WorkflowType,
		TaskQueue:    i.TaskQueue,
		Status:       i.Status,
		StartTime:    timestamppb.New(i.StartTime),
	}
	if !i.CloseTime.IsZero() {
		pb.CloseTime = timestamppb.New(i.CloseTime)
	}
	return pb
}

// statusToString is statusFromString's inverse, used to translate an
// optional ListWorkflowExecutions status filter into the DB's status
// column values (see the workflow_executions_status_check constraint).
// UNSPECIFIED has no string form — callers must not pass it here (see
// ListWorkflowExecutionsParams.StatusFilter's zero-value-means-"no
// filter" handling above).
func statusToString(s flowv1.WorkflowExecutionStatus) string {
	switch s {
	case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "RUNNING"
	case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "COMPLETED"
	case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_FAILED:
		return "FAILED"
	case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "TERMINATED"
	case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "TIMED_OUT"
	case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "CONTINUED_AS_NEW"
	default:
		return ""
	}
}
