package workflow

import (
	"fmt"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/funcname"
)

// ContinueAsNewOptions configures a continue-as-new call. A zero value
// means "same as the current run" for every field.
type ContinueAsNewOptions struct {
	// TaskQueue empty continues on the same task queue as the current run.
	TaskQueue string
	// RetryPolicy nil continues with no workflow-level retry policy
	// recorded on the new run's start event (informational only — see
	// ContinueAsNewWorkflowExecutionCommand's doc; nothing in flowd reads
	// it back as an activity default).
	RetryPolicy *flowv1.RetryPolicy
	// WorkflowRunTimeout/WorkflowTaskTimeout zero continues with the
	// current run's own values.
	WorkflowRunTimeout  time.Duration
	WorkflowTaskTimeout time.Duration
}

// ContinueAsNewError, returned by a workflow function, tells the worker to
// close this run and atomically start a fresh run under the same
// workflow_id instead of completing normally — the way to write a
// workflow that legitimately loops forever without its history growing
// without bound (see Phase 2 roadmap, Track B, item 2). Construct it with
// NewContinueAsNewError; it must be returned directly (not wrapped), since
// the worker detects it with errors.As on the workflow function's return
// value.
type ContinueAsNewError struct {
	WorkflowType string
	Input        any
	Options      ContinueAsNewOptions
}

func (e *ContinueAsNewError) Error() string {
	return fmt.Sprintf("workflow: continue as new (%s)", e.WorkflowType)
}

// NewContinueAsNewError builds a ContinueAsNewError for a workflow
// function to return, continuing this workflow_id as a fresh run of
// workflowFn with arg as its new input. workflowFn is never invoked
// locally — reflection only derives its registered name, the same
// funcname.Of convention client.StartWorkflow and ExecuteActivity use, so
// it matches whatever name worker.RegisterWorkflow registered without the
// caller hardcoding a string. Continuing to a different workflow function
// than the one currently running is legal (its own registered name is
// simply used instead), the same as real continue-as-new semantics.
func NewContinueAsNewError(workflowFn any, arg any, opts ContinueAsNewOptions) error {
	return &ContinueAsNewError{WorkflowType: funcname.Of(workflowFn), Input: arg, Options: opts}
}
