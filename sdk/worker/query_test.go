package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/activity"
	"github.com/krishnakichuu/flowd/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// queryTestInvocations counts how many times queryTestWorkflow's body ran
// from the top — same proof technique as sticky_test.go: a cache-hit query
// must answer without re-running the workflow function at all.
var queryTestInvocations int32

func queryTestWorkflow(ctx workflow.Context, remaining int) (string, error) {
	atomic.AddInt32(&queryTestInvocations, 1)
	if err := workflow.SetQueryHandler(ctx, "remaining", func(struct{}) (int, error) {
		return remaining, nil
	}); err != nil {
		return "", err
	}
	var out string
	err := workflow.ExecuteActivity(ctx, queryTestActivity, remaining, workflow.ActivityOptions{}).Get(&out)
	return out, err
}

func queryTestActivity(_ activity.Context, remaining int) (string, error) {
	return "done", nil
}

// queryFakeClient records every RespondWorkflowTaskCompleted and
// RespondQueryTaskCompleted call it receives.
type queryFakeClient struct {
	flowv1.WorkflowServiceClient
	completedWorkflow []*flowv1.RespondWorkflowTaskCompletedRequest
	completedQuery    []*flowv1.RespondQueryTaskCompletedRequest
}

func (f *queryFakeClient) RespondWorkflowTaskCompleted(_ context.Context, req *flowv1.RespondWorkflowTaskCompletedRequest, _ ...grpc.CallOption) (*flowv1.RespondWorkflowTaskCompletedResponse, error) {
	f.completedWorkflow = append(f.completedWorkflow, req)
	return &flowv1.RespondWorkflowTaskCompletedResponse{}, nil
}

func (f *queryFakeClient) RespondQueryTaskCompleted(_ context.Context, req *flowv1.RespondQueryTaskCompletedRequest, _ ...grpc.CallOption) (*flowv1.RespondQueryTaskCompletedResponse, error) {
	f.completedQuery = append(f.completedQuery, req)
	return &flowv1.RespondQueryTaskCompletedResponse{}, nil
}

func newQueryTestWorker(fake *queryFakeClient) *Worker {
	w := &Worker{
		rpc:                          fake,
		identity:                     "worker-under-test",
		workflows:                    make(map[string]workflowFunc),
		activities:                   make(map[string]activityFunc),
		execCache:                    newExecutionCache(defaultMaxCachedWorkflowExecutions),
		stickyScheduleToStartTimeout: 5 * time.Second,
		logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	w.RegisterWorkflow(queryTestWorkflow)
	w.RegisterActivity(queryTestActivity)
	return w
}

func startedHistory(t *testing.T, remaining int) []*flowv1.HistoryEvent {
	t.Helper()
	data, err := json.Marshal(remaining)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	return []*flowv1.HistoryEvent{
		{EventId: 1, Attributes: &flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &flowv1.WorkflowExecutionStartedEventAttributes{
				WorkflowType: "queryTestWorkflow", Input: &flowv1.Payload{Data: data},
			},
		}},
		{EventId: 2, Attributes: &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &flowv1.WorkflowTaskScheduledEventAttributes{},
		}},
		{EventId: 3, EventTime: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
		}},
	}
}

// TestProcessQueryTaskCacheHitSkipsReplay proves a query against an
// already-cached run answers straight from the live Execution — no
// re-running the workflow function, no LoadNewEvents/ExecuteRound at all.
func TestProcessQueryTaskCacheHitSkipsReplay(t *testing.T) {
	atomic.StoreInt32(&queryTestInvocations, 0)
	fake := &queryFakeClient{}
	w := newQueryTestWorker(fake)
	exec := flowv1.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"}
	history := startedHistory(t, 5)
	ctx := context.Background()

	// Task 1: a normal workflow task — cache miss, schedules an activity,
	// and gets cached since the run is still open.
	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("task-1"), WorkflowExecution: &exec, WorkflowType: "queryTestWorkflow", History: history,
	})
	if got := atomic.LoadInt32(&queryTestInvocations); got != 1 {
		t.Fatalf("invocations after task 1 = %d, want 1", got)
	}
	if w.execCache.len() != 1 {
		t.Fatalf("cache len after task 1 = %d, want 1", w.execCache.len())
	}

	// A query for the same run: same History (nothing new happened),
	// QueryTask set instead of a normal dispatch.
	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("query-1"), WorkflowExecution: &exec, WorkflowType: "queryTestWorkflow", History: history,
		QueryTask: &flowv1.QueryTask{QueryType: "remaining"},
	})

	if got := atomic.LoadInt32(&queryTestInvocations); got != 1 {
		t.Fatalf("invocations after the query = %d, want 1 (the query must not re-run the workflow body)", got)
	}
	if len(fake.completedWorkflow) != 1 {
		t.Fatalf("RespondWorkflowTaskCompleted calls = %d, want 1 (only from task 1)", len(fake.completedWorkflow))
	}
	if len(fake.completedQuery) != 1 {
		t.Fatalf("RespondQueryTaskCompleted calls = %d, want 1", len(fake.completedQuery))
	}
	resp := fake.completedQuery[0]
	if resp.Failure != nil {
		t.Fatalf("query failed: %v", resp.Failure)
	}
	var got int
	if err := json.Unmarshal(resp.Result.Data, &got); err != nil {
		t.Fatalf("unmarshal query result: %v", err)
	}
	if got != 5 {
		t.Fatalf("query result = %d, want 5", got)
	}
}

// TestProcessQueryTaskCacheMissReplaysAndCaches proves a query against a
// run this worker has never seen rebuilds state via a full replay (not an
// error), answers correctly, and opportunistically caches the rebuild.
func TestProcessQueryTaskCacheMissReplaysAndCaches(t *testing.T) {
	atomic.StoreInt32(&queryTestInvocations, 0)
	fake := &queryFakeClient{}
	w := newQueryTestWorker(fake)
	exec := flowv1.WorkflowExecution{WorkflowId: "wf-2", RunId: "run-2"}
	history := startedHistory(t, 7)

	w.processWorkflowTask(context.Background(), &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("query-1"), WorkflowExecution: &exec, WorkflowType: "queryTestWorkflow", History: history,
		QueryTask: &flowv1.QueryTask{QueryType: "remaining"},
	})

	if got := atomic.LoadInt32(&queryTestInvocations); got != 1 {
		t.Fatalf("invocations = %d, want 1 (a cache miss must replay once to rebuild state)", got)
	}
	if len(fake.completedQuery) != 1 || fake.completedQuery[0].Failure != nil {
		t.Fatalf("completedQuery = %+v, want one successful response", fake.completedQuery)
	}
	var got int
	if err := json.Unmarshal(fake.completedQuery[0].Result.Data, &got); err != nil {
		t.Fatalf("unmarshal query result: %v", err)
	}
	if got != 7 {
		t.Fatalf("query result = %d, want 7", got)
	}
	if w.execCache.len() != 1 {
		t.Fatalf("cache len = %d, want 1 (the replay-to-answer should be cached opportunistically)", w.execCache.len())
	}
}

// TestProcessQueryTaskUnregisteredTypeFails proves an unknown query_type
// comes back as a query failure, not a crash or a silently wrong answer.
func TestProcessQueryTaskUnregisteredTypeFails(t *testing.T) {
	atomic.StoreInt32(&queryTestInvocations, 0)
	fake := &queryFakeClient{}
	w := newQueryTestWorker(fake)
	exec := flowv1.WorkflowExecution{WorkflowId: "wf-3", RunId: "run-3"}

	w.processWorkflowTask(context.Background(), &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("query-1"), WorkflowExecution: &exec, WorkflowType: "queryTestWorkflow", History: startedHistory(t, 1),
		QueryTask: &flowv1.QueryTask{QueryType: "does-not-exist"},
	})

	if len(fake.completedQuery) != 1 {
		t.Fatalf("completedQuery = %+v, want exactly 1 response", fake.completedQuery)
	}
	resp := fake.completedQuery[0]
	if resp.Failure == nil {
		t.Fatal("expected a Failure for an unregistered query type, got a successful result")
	}
	if resp.Result != nil {
		t.Fatalf("Result = %v, want nil alongside a Failure", resp.Result)
	}
}
