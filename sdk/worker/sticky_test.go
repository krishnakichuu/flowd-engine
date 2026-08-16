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

// stickyTestInvocations counts how many times stickyTestWorkflow's body ran
// from the top — the sharpest possible proof a cache hit actually resumed
// the cached coroutine instead of a full replay: a full replay of history
// that already recorded the activity's outcome would produce the exact
// same final result, so only an invocation count distinguishes "resumed"
// from "coincidentally replayed to the same answer."
var stickyTestInvocations int32

func stickyTestWorkflow(ctx workflow.Context, name string) (string, error) {
	atomic.AddInt32(&stickyTestInvocations, 1)
	var out string
	err := workflow.ExecuteActivity(ctx, stickyTestActivity, name, workflow.ActivityOptions{}).Get(&out)
	return out, err
}

func stickyTestActivity(_ activity.Context, name string) (string, error) {
	return "hello " + name, nil
}

// stickyFakeClient records every RespondWorkflowTaskCompleted call it
// receives; processWorkflowTask is invoked directly with hand-built poll
// responses rather than through a poll loop, so this only needs to answer
// the completion RPC.
type stickyFakeClient struct {
	flowv1.WorkflowServiceClient
	completed []*flowv1.RespondWorkflowTaskCompletedRequest
}

func (f *stickyFakeClient) RespondWorkflowTaskCompleted(_ context.Context, req *flowv1.RespondWorkflowTaskCompletedRequest, _ ...grpc.CallOption) (*flowv1.RespondWorkflowTaskCompletedResponse, error) {
	f.completed = append(f.completed, req)
	return &flowv1.RespondWorkflowTaskCompletedResponse{}, nil
}

func jsonPayload(t *testing.T, v any) *flowv1.Payload {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &flowv1.Payload{Data: data}
}

// TestProcessWorkflowTaskStickyResumeSkipsReplay drives the same run
// through two calls to processWorkflowTask directly — task 1 (a cache
// miss: schedules an activity) and task 2 (a cache hit: the activity's
// outcome arrives, along with events task 1 never saw) — and checks both
// the observable behavior (correct final result, exactly one Command per
// response, sticky attributes only while the run is still open) and,
// critically, that the workflow function's body only ran once total.
func TestProcessWorkflowTaskStickyResumeSkipsReplay(t *testing.T) {
	atomic.StoreInt32(&stickyTestInvocations, 0)

	exec := flowv1.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"}
	t1 := timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	t2 := timestamppb.New(time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC))

	task1History := []*flowv1.HistoryEvent{
		{EventId: 1, Attributes: &flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &flowv1.WorkflowExecutionStartedEventAttributes{
				WorkflowType: "stickyTestWorkflow", Input: jsonPayload(t, "world"),
			},
		}},
		{EventId: 2, Attributes: &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &flowv1.WorkflowTaskScheduledEventAttributes{},
		}},
		{EventId: 3, EventTime: t1, Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
		}},
	}

	// task2History is task1History's exact prefix plus the tail a real
	// server would have appended by the time the activity completed and a
	// second workflow task was dispatched.
	task2History := append(
		append([]*flowv1.HistoryEvent{}, task1History...),
		&flowv1.HistoryEvent{EventId: 4, Attributes: &flowv1.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &flowv1.WorkflowTaskCompletedEventAttributes{},
		}},
		&flowv1.HistoryEvent{EventId: 5, Attributes: &flowv1.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &flowv1.ActivityTaskScheduledEventAttributes{ActivityId: 1, ActivityType: "stickyTestActivity"},
		}},
		&flowv1.HistoryEvent{EventId: 6, Attributes: &flowv1.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &flowv1.ActivityTaskCompletedEventAttributes{ActivityId: 1, Result: jsonPayload(t, "hello world")},
		}},
		&flowv1.HistoryEvent{EventId: 7, Attributes: &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &flowv1.WorkflowTaskScheduledEventAttributes{},
		}},
		&flowv1.HistoryEvent{EventId: 8, EventTime: t2, Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
		}},
	)

	fake := &stickyFakeClient{}
	w := &Worker{
		rpc:                          fake,
		identity:                     "worker-under-test",
		workflows:                    make(map[string]workflowFunc),
		activities:                   make(map[string]activityFunc),
		execCache:                    newExecutionCache(defaultMaxCachedWorkflowExecutions),
		stickyScheduleToStartTimeout: 5 * time.Second,
		logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	w.RegisterWorkflow(stickyTestWorkflow)
	w.RegisterActivity(stickyTestActivity)

	ctx := context.Background()

	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("task-1"), WorkflowExecution: &exec, WorkflowType: "stickyTestWorkflow", History: task1History,
	})

	if got := w.execCache.len(); got != 1 {
		t.Fatalf("after task 1, cache len = %d, want 1 (still running)", got)
	}
	if len(fake.completed) != 1 {
		t.Fatalf("expected 1 RespondWorkflowTaskCompleted call after task 1, got %d", len(fake.completed))
	}
	resp1 := fake.completed[0]
	if len(resp1.Commands) != 1 {
		t.Fatalf("task 1 commands = %v, want exactly 1 (ScheduleActivityTask)", resp1.Commands)
	}
	if _, ok := resp1.Commands[0].Command.(*flowv1.Command_ScheduleActivityTask); !ok {
		t.Fatalf("task 1 command is %T, want ScheduleActivityTask", resp1.Commands[0].Command)
	}
	if resp1.StickyExecutionAttributes == nil || resp1.StickyExecutionAttributes.WorkerIdentity != "worker-under-test" {
		t.Fatalf("task 1 sticky attributes = %v, want WorkerIdentity=worker-under-test", resp1.StickyExecutionAttributes)
	}
	if got := atomic.LoadInt32(&stickyTestInvocations); got != 1 {
		t.Fatalf("workflow body ran %d times after task 1, want 1", got)
	}

	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("task-2"), WorkflowExecution: &exec, WorkflowType: "stickyTestWorkflow", History: task2History,
	})

	// The sharpest assertion: the workflow function's body must not have
	// run again. If task 2 had fallen back to a full replay instead of
	// resuming the cached coroutine, this would be 2, not 1 — and the
	// final result would still happen to be correct either way, which is
	// exactly why this counter, not just the result, is what proves the
	// fast path was actually taken.
	if got := atomic.LoadInt32(&stickyTestInvocations); got != 1 {
		t.Fatalf("workflow body ran %d times total, want 1 (task 2 should have resumed, not replayed)", got)
	}

	if got := w.execCache.len(); got != 0 {
		t.Fatalf("after task 2 (terminal), cache len = %d, want 0", got)
	}
	if len(fake.completed) != 2 {
		t.Fatalf("expected 2 RespondWorkflowTaskCompleted calls total, got %d", len(fake.completed))
	}
	resp2 := fake.completed[1]
	if len(resp2.Commands) != 1 {
		t.Fatalf("task 2 commands = %v, want exactly 1 (CompleteWorkflowExecution) — a stale ScheduleActivityTask from task 1 would mean NewCommands wasn't reset", resp2.Commands)
	}
	complete, ok := resp2.Commands[0].Command.(*flowv1.Command_CompleteWorkflowExecution)
	if !ok {
		t.Fatalf("task 2 command is %T, want CompleteWorkflowExecution", resp2.Commands[0].Command)
	}
	var result string
	if err := json.Unmarshal(complete.CompleteWorkflowExecution.Result.Data, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result != "hello world" {
		t.Fatalf("result = %q, want %q", result, "hello world")
	}
	if resp2.StickyExecutionAttributes != nil {
		t.Fatalf("task 2 (terminal) sticky attributes = %v, want nil", resp2.StickyExecutionAttributes)
	}
}
