package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/proto"
)

// AppendHistory is the correctness chokepoint for every write to a
// workflow's event history (ADR-0002, Mechanism B). Every other write path
// in this package (StartWorkflowExecution, CompleteWorkflowTask,
// CompleteActivityTask, FailActivityTask, SignalWorkflowExecution, timer
// firing) must go through this function rather than inserting into
// history_events directly.
//
// `expected` must be the next_event_id the caller observed earlier in the
// same transaction (e.g. via GetWorkflowExecution). If another writer has
// since advanced the counter, this returns ErrConcurrentModification and the
// caller must abort its transaction and discard any in-memory replay state
// — it must not retry blindly, since the workflow's state may have changed
// underneath it.
func AppendHistory(ctx context.Context, q *sqlc.Queries, namespaceID int64, workflowID, runID string, expected int64, events []NewEvent) (newNextEventID int64, assignedEventIDs []int64, err error) {
	if len(events) == 0 {
		return expected, nil, nil
	}

	newNext, err := q.AdvanceNextEventID(ctx, sqlc.AdvanceNextEventIDParams{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		Increment:   int64(len(events)),
		Expected:    expected,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, ErrConcurrentModification
		}
		return 0, nil, fmt.Errorf("history: advance next_event_id: %w", err)
	}

	ids := make([]int64, len(events))
	for i, ev := range events {
		id := expected + int64(i)
		ids[i] = id

		attrBytes, err := proto.Marshal(ev.Attributes)
		if err != nil {
			return 0, nil, fmt.Errorf("history: marshal attributes for event type %s: %w", ev.EventType, err)
		}

		if err := q.InsertHistoryEvent(ctx, sqlc.InsertHistoryEventParams{
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			EventID:     id,
			EventType:   ev.EventType.String(),
			Attributes:  attrBytes,
		}); err != nil {
			return 0, nil, fmt.Errorf("history: insert event %d: %w", id, err)
		}
	}

	return newNext, ids, nil
}
