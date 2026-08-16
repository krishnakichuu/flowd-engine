package worker

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/replayer"
	"github.com/krishnakichuu/flowd/sdk/workflow"
)

// loopWorkflow is what LoopingWorkflow's *next* run would be registered
// as — runWorkflowFunc/funcname only need a real function reference to
// derive a name from, it is never called directly here.
func loopWorkflow(_ workflow.Context, _ string) (string, error) { return "", nil }

// loopingWorkflow always continues as new instead of returning normally.
func loopingWorkflow(_ workflow.Context, in string) (string, error) {
	return "", workflow.NewContinueAsNewError(loopWorkflow, in+"-next", workflow.ContinueAsNewOptions{
		TaskQueue:          "q2",
		WorkflowRunTimeout: time.Minute,
	})
}

func TestRunWorkflowFuncRoutesContinueAsNewError(t *testing.T) {
	exec := replayer.NewExecution()
	fn := reflect.ValueOf(loopingWorkflow)

	input, err := json.Marshal("start")
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	if err := runWorkflowFunc(exec, fn, fn.Type().In(1), &flowv1.Payload{Data: input}); err != nil {
		t.Fatalf("runWorkflowFunc: %v", err)
	}

	if exec.Result != nil || exec.Err != nil {
		t.Fatalf("a continue-as-new return must not be treated as Complete/Fail, got Result=%v Err=%v", exec.Result, exec.Err)
	}
	if exec.ContinuedAsNew == nil {
		t.Fatal("ContinuedAsNew was not set")
	}
	if got := exec.ContinuedAsNew.WorkflowType; got != "loopWorkflow" {
		t.Fatalf("WorkflowType = %q, want %q", got, "loopWorkflow")
	}
	if got := exec.ContinuedAsNew.TaskQueue; got != "q2" {
		t.Fatalf("TaskQueue = %q, want %q", got, "q2")
	}
	if got := exec.ContinuedAsNew.WorkflowRunTimeout.AsDuration(); got != time.Minute {
		t.Fatalf("WorkflowRunTimeout = %v, want %v", got, time.Minute)
	}

	var gotInput string
	if err := json.Unmarshal(exec.ContinuedAsNew.Input.Data, &gotInput); err != nil {
		t.Fatalf("unmarshal recorded input: %v", err)
	}
	if gotInput != "start-next" {
		t.Fatalf("recorded input = %q, want %q", gotInput, "start-next")
	}

	if len(exec.NewCommands) != 1 {
		t.Fatalf("NewCommands has %d entries, want exactly 1", len(exec.NewCommands))
	}
	if _, ok := exec.NewCommands[0].Command.(*flowv1.Command_ContinueAsNewWorkflowExecution); !ok {
		t.Fatalf("NewCommands[0] is %T, want *flowv1.Command_ContinueAsNewWorkflowExecution", exec.NewCommands[0].Command)
	}
}

func TestReplayWorkflowHistoryReturnsContinuedAsNewError(t *testing.T) {
	start := &flowv1.HistoryEvent{
		EventType: flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &flowv1.WorkflowExecutionStartedEventAttributes{
				WorkflowType: "loopingWorkflow",
				Input:        &flowv1.Payload{Data: []byte(`"start"`)},
			},
		},
	}

	_, err := ReplayWorkflowHistory([]*flowv1.HistoryEvent{start}, loopingWorkflow)
	if err == nil {
		t.Fatal("expected a ContinuedAsNewError, got nil")
	}
	var canErr *ContinuedAsNewError
	if !errors.As(err, &canErr) {
		t.Fatalf("expected a *ContinuedAsNewError, got %T: %v", err, err)
	}
	if canErr.WorkflowType != "loopWorkflow" {
		t.Fatalf("WorkflowType = %q, want %q", canErr.WorkflowType, "loopWorkflow")
	}
}
