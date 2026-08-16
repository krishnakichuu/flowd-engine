package worker

import (
	"errors"
	"fmt"
	"reflect"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
	"github.com/krishnakichuu/flowd/sdk/internal/replayer"
	"github.com/krishnakichuu/flowd/sdk/workflow"
)

// IsNonDeterministic reports whether err (as returned by
// ReplayWorkflowHistory or observed via a worker's logs) is a replay
// non-determinism mismatch rather than an ordinary workflow failure or
// panic. replayer.NonDeterministicError itself is unexported (an SDK
// internal), so this is the public way to check the condition.
func IsNonDeterministic(err error) bool {
	var ndErr *replayer.NonDeterministicError
	return errors.As(err, &ndErr)
}

// ContinuedAsNewError is returned by ReplayWorkflowHistory when the
// workflow function, as currently coded, continues as new rather than
// completing at the point events ends — a golden-fixture replay can't
// assert a result past that boundary (the continuation's own run has its
// own, separate history), so this is surfaced distinctly rather than as a
// misleading (nil, nil) success.
type ContinuedAsNewError struct{ WorkflowType string }

func (e *ContinuedAsNewError) Error() string {
	return fmt.Sprintf("workflow continued as new as %q instead of completing", e.WorkflowType)
}

// runWorkflowFunc drives fn against exec through a single dispatcher round
// — the shared replay/execution primitive behind both
// Worker.processWorkflowTask (a live task) and ReplayWorkflowHistory (a
// standalone replay test). It returns an error only for an input unmarshal
// failure; a workflow panic (including non-determinism) is instead left on
// exec.Dispatcher, and the workflow's own result/failure on exec itself,
// since both callers need to distinguish those cases differently.
func runWorkflowFunc(exec *replayer.Execution, fn reflect.Value, inputType reflect.Type, input *flowv1.Payload) error {
	inputPtr := reflect.New(inputType)
	if err := converter.Default.FromPayload(input, inputPtr.Interface()); err != nil {
		return fmt.Errorf("input unmarshal: %w", err)
	}

	exec.Dispatcher.Go(func(c *replayer.Coroutine) {
		wctx := workflow.NewContext(exec, c)
		out := fn.Call([]reflect.Value{reflect.ValueOf(wctx), inputPtr.Elem()})

		errVal := out[1]
		if errVal.IsNil() {
			payload, err := converter.Default.ToPayload(out[0].Interface())
			if err != nil {
				exec.Complete(nil, &flowv1.Failure{Message: err.Error(), Type: "MarshalError"})
				return
			}
			exec.Complete(payload, nil)
			return
		}

		err := errVal.Interface().(error)
		var canErr *workflow.ContinueAsNewError
		if errors.As(err, &canErr) {
			payload, perr := converter.Default.ToPayload(canErr.Input)
			if perr != nil {
				exec.Complete(nil, &flowv1.Failure{Message: perr.Error(), Type: "MarshalError"})
				return
			}
			exec.ContinueAsNew(canErr.WorkflowType, payload, replayer.ContinueAsNewOptions{
				TaskQueue:           canErr.Options.TaskQueue,
				RetryPolicy:         canErr.Options.RetryPolicy,
				WorkflowRunTimeout:  canErr.Options.WorkflowRunTimeout,
				WorkflowTaskTimeout: canErr.Options.WorkflowTaskTimeout,
			})
			return
		}

		exec.Complete(nil, &flowv1.Failure{Message: err.Error(), Type: fmt.Sprintf("%T", err)})
	})

	exec.Dispatcher.ExecuteRound()
	return nil
}

// ReplayWorkflowHistory drives workflowFn — a func(workflow.Context, In)
// (Out, error) — against a fixed history using the same deterministic
// replay algorithm a live worker uses, with no server or task queue
// involved. This is the SDK's golden-replay testing primitive: capture a
// real run's history once, check it in, and assert replaying it against
// the current workflow code still produces the same result — the
// earliest signal that a workflow code change breaks replay of
// already-running executions (ADR-0001). A non-determinism mismatch
// surfaces as a *replayer.NonDeterministicError.
func ReplayWorkflowHistory(events []*flowv1.HistoryEvent, workflowFn any) (*flowv1.Payload, error) {
	v := reflect.ValueOf(workflowFn)
	mustBeSDKFunc(v.Type())

	exec := replayer.NewExecution()
	input, _ := exec.LoadHistory(events)

	if err := runWorkflowFunc(exec, v, v.Type().In(1), input); err != nil {
		return nil, err
	}
	if panicErr := exec.Dispatcher.FirstPanic(); panicErr != nil {
		return nil, panicErr
	}
	if exec.ContinuedAsNew != nil {
		return nil, &ContinuedAsNewError{WorkflowType: exec.ContinuedAsNew.WorkflowType}
	}
	if exec.Err != nil {
		return nil, fmt.Errorf("workflow failed: %s", exec.Err.Message)
	}
	return exec.Result, nil
}
