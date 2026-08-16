package history

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func pgUUIDFromString(s string) pgtype.UUID {
	parsed, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}
}

// stringOrFallback is ContinueAsNewWorkflowExecutionCommand's "unset means
// continue with the current run's value" rule: an empty workflow_type or
// task_queue on the command means "same as the run that's continuing".
func stringOrFallback(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

// durationOrFallback is the same "unset means continue with the current
// run's value" rule for ContinueAsNewWorkflowExecutionCommand's
// workflow_run_timeout/workflow_task_timeout, which fall back to the
// current run's stored value (already a pgtype.Int8) rather than a Go
// duration.
func durationOrFallback(v *durationpb.Duration, fallback pgtype.Int8) pgtype.Int8 {
	if v == nil {
		return fallback
	}
	return durationToPgInt8(v.AsDuration())
}

// stickyEnqueueFields resolves what a workflow_tasks row's
// preferred_worker_identity/sticky_deadline should be for enqueueing we's
// next workflow task: the run's registered sticky worker (see
// SetStickyExecution) if that registration hasn't already expired, or the
// zero value (no preference — every worker can see it, today's original
// behavior) otherwise. we.StickyExpiresAt is an absolute timestamp fixed
// at the moment the worker last responded, not a window that restarts
// here — if the next event that needs a workflow task doesn't happen
// until after that time (e.g. a long timer), this run has already fallen
// out of stickiness by the time we're enqueueing.
func stickyEnqueueFields(we sqlc.WorkflowExecution) (pgtype.Text, pgtype.Timestamptz) {
	if !we.StickyWorkerIdentity.Valid || !we.StickyExpiresAt.Valid {
		return pgtype.Text{}, pgtype.Timestamptz{}
	}
	if we.StickyExpiresAt.Time.Before(time.Now()) {
		return pgtype.Text{}, pgtype.Timestamptz{}
	}
	return we.StickyWorkerIdentity, we.StickyExpiresAt
}

// activityEnqueueParams builds the activity_tasks insert for a
// ScheduleActivityTaskCommand, marshaling its Payload input to bytes and
// carrying its RetryPolicy/timeouts into the queue row so DequeueActivityTask
// can derive the lease length from this activity's own start_to_close_timeout
// (ADR-0002) rather than a fixed constant.
func activityEnqueueParams(namespaceID int64, taskQueue string, taskQueuePartition int32, workflowID, runID string, cmd *flowv1.ScheduleActivityTaskCommand) sqlc.EnqueueActivityTaskParams {
	inputBytes, _ := proto.Marshal(cmd.Input)

	rp := cmd.RetryPolicy
	var initialNs, maxIntervalNs int64
	coeff := 2.0
	var maxAttempts int32
	nonRetryable := []string{}
	if rp != nil {
		if rp.InitialInterval != nil {
			initialNs = rp.InitialInterval.AsDuration().Nanoseconds()
		}
		if rp.MaxInterval != nil {
			maxIntervalNs = rp.MaxInterval.AsDuration().Nanoseconds()
		}
		if rp.BackoffCoefficient > 0 {
			coeff = rp.BackoffCoefficient
		}
		maxAttempts = rp.MaxAttempts
		nonRetryable = rp.NonRetryableErrorTypes
	}

	var scheduleToStartNs, startToCloseNs int64
	if cmd.ScheduleToStartTimeout != nil {
		scheduleToStartNs = cmd.ScheduleToStartTimeout.AsDuration().Nanoseconds()
	}
	if cmd.StartToCloseTimeout != nil {
		startToCloseNs = cmd.StartToCloseTimeout.AsDuration().Nanoseconds()
	}
	if startToCloseNs <= 0 {
		startToCloseNs = int64(defaultActivityStartToCloseTimeout)
	}

	return sqlc.EnqueueActivityTaskParams{
		NamespaceID:              namespaceID,
		TaskQueueName:            taskQueue,
		TaskQueuePartition:       taskQueuePartition,
		WorkflowID:               workflowID,
		RunID:                    runID,
		ActivityID:               cmd.ActivityId,
		ActivityType:             cmd.ActivityType,
		ScheduledEventID:         0, // set by caller if/when per-event tracking is needed
		Input:                    inputBytes,
		ScheduleToStartTimeoutNs: scheduleToStartNs,
		StartToCloseTimeoutNs:    startToCloseNs,
		RetryInitialIntervalNs:   initialNs,
		RetryBackoffCoefficient:  coeff,
		RetryMaxIntervalNs:       maxIntervalNs,
		RetryMaxAttempts:         maxAttempts,
		RetryNonRetryableTypes:   nonRetryable,
	}
}
