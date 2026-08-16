package history

import "time"

const (
	// workflowTaskLeaseSeconds is how long a dequeued workflow task's lease
	// lasts before the reaper reclaims it (default from the plan).
	workflowTaskLeaseSeconds = 10

	// defaultActivityStartToCloseTimeout is used when a ScheduleActivityTask
	// command omits start_to_close_timeout; DequeueActivityTask derives its
	// lease length from this value (ADR-0002).
	defaultActivityStartToCloseTimeout = 30 * time.Second
)
