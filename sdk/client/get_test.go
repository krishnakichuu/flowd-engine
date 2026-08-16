package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
	"google.golang.org/grpc"
)

// fakeGetClient implements flowv1.WorkflowServiceClient by embedding it
// (only DescribeWorkflowExecution/GetWorkflowExecutionHistory are ever
// called on this path) and simulates a workflow_id whose original run
// continued as new: describing run_id "run1" returns CONTINUED_AS_NEW, and
// describing an empty run_id (current_executions resolution) walks
// currentSeq forward one step per call — run2 (still continuing), then
// run3 (COMPLETED) — the same multi-hop chain a real server would produce
// across repeated ContinueAsNew calls.
type fakeGetClient struct {
	flowv1.WorkflowServiceClient
	byRunID    map[string]*flowv1.DescribeWorkflowExecutionResponse
	currentSeq []*flowv1.DescribeWorkflowExecutionResponse
	currentIdx int
	history    map[string][]*flowv1.HistoryEvent
}

func (f *fakeGetClient) DescribeWorkflowExecution(_ context.Context, req *flowv1.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*flowv1.DescribeWorkflowExecutionResponse, error) {
	if req.RunId == "" {
		resp := f.currentSeq[f.currentIdx]
		if f.currentIdx < len(f.currentSeq)-1 {
			f.currentIdx++
		}
		return resp, nil
	}
	resp, ok := f.byRunID[req.RunId]
	if !ok {
		return nil, fmt.Errorf("fakeGetClient: no response for run_id %q", req.RunId)
	}
	return resp, nil
}

func (f *fakeGetClient) GetWorkflowExecutionHistory(_ context.Context, req *flowv1.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*flowv1.GetWorkflowExecutionHistoryResponse, error) {
	return &flowv1.GetWorkflowExecutionHistoryResponse{Events: f.history[req.RunId]}, nil
}

// TestWorkflowRunGetFollowsContinueAsNewChain proves the fix directly: Get
// must not treat CONTINUED_AS_NEW as terminal (it would otherwise poll
// run1 forever, since a continued run's status never changes again). It
// should follow the chain — through however many further continuations —
// to the run that actually finishes, fetch its result, and leave RunID
// pointing at that run.
func TestWorkflowRunGetFollowsContinueAsNewChain(t *testing.T) {
	resultPayload, err := converter.Default.ToPayload("final")
	if err != nil {
		t.Fatalf("build result payload: %v", err)
	}

	fake := &fakeGetClient{
		byRunID: map[string]*flowv1.DescribeWorkflowExecutionResponse{
			"run1": {
				Execution: &flowv1.WorkflowExecution{WorkflowId: "wf", RunId: "run1"},
				Status:    flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
		},
		currentSeq: []*flowv1.DescribeWorkflowExecutionResponse{
			{
				Execution: &flowv1.WorkflowExecution{WorkflowId: "wf", RunId: "run2"},
				Status:    flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
			{
				Execution: &flowv1.WorkflowExecution{WorkflowId: "wf", RunId: "run3"},
				Status:    flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		history: map[string][]*flowv1.HistoryEvent{
			"run3": {{
				Attributes: &flowv1.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &flowv1.WorkflowExecutionCompletedEventAttributes{Result: resultPayload},
				},
			}},
		},
	}

	c := &Client{rpc: fake, namespace: "default"}
	run := &WorkflowRun{client: c, WorkflowID: "wf", RunID: "run1"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var got string
	if err := run.Get(ctx, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "final" {
		t.Fatalf("result = %q, want %q", got, "final")
	}
	if run.RunID != "run3" {
		t.Fatalf("RunID = %q, want %q (Get should settle on the run that actually completed)", run.RunID, "run3")
	}
}
