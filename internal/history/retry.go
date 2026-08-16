package history

import (
	"context"
	"errors"

	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// maxConcurrentModificationRetries bounds how many times an out-of-band
// write (SignalWorkflowExecution, RequestCancelWorkflowExecution) retries
// after losing the next_event_id CAS to a concurrently-completing
// workflow task — e.g. a running workflow's own timer-driven task
// advances history at the same moment a client signals or cancels it.
// Unlike a worker's own RespondWorkflowTaskCompleted (which must NOT
// blindly retry after ErrConcurrentModification — its commands were
// computed against a workflow-code snapshot that's now stale, see
// AppendHistory's doc), Signal/Cancel write fixed-content events that
// don't depend on the specific next_event_id value or anything else about
// prior state, so re-reading the current execution and trying again is
// safe here specifically, not just convenient.
const maxConcurrentModificationRetries = 5

// withTxRetryOnConflict runs fn inside a transaction (via s.withTx),
// retrying up to maxConcurrentModificationRetries times on
// ErrConcurrentModification — see that constant's doc for why this retry
// is safe for its callers. Any other error, or exhausting the retries,
// returns immediately.
func (s *Store) withTxRetryOnConflict(ctx context.Context, fn func(q *sqlc.Queries) error) error {
	var err error
	for attempt := 0; attempt < maxConcurrentModificationRetries; attempt++ {
		err = s.withTx(ctx, fn)
		if !errors.Is(err, ErrConcurrentModification) {
			return err
		}
	}
	return err
}
