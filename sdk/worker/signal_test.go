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
	"github.com/krishnakichuu/flowd/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// signalTestPaymentID records what the signal handler saw, from outside
// the workflow closure — the same "observe via a package-level var, since
// the workflow function's own locals aren't reachable from the test"
// technique stickyTestInvocations/queryTestInvocations already use.
var signalTestPaymentID atomic.Value

// signalTestWorkflow registers a handler near the top (before its first
// blocking call, per SetSignalHandler's documented convention) and then
// polls for the signal the same way a workflow observes any state a
// signal handler can only set, never block on directly: a loop around a
// blocking primitive (here, Sleep — the same pattern IsCancelRequested's
// own doc points to).
func signalTestWorkflow(ctx workflow.Context, _ struct{}) (string, error) {
	var received string
	if err := workflow.SetSignalHandler(ctx, "payment", func(paymentID string) {
		received = paymentID
		signalTestPaymentID.Store(paymentID)
	}); err != nil {
		return "", err
	}
	for received == "" {
		workflow.Sleep(ctx, time.Second)
	}
	return received, nil
}

type signalFakeClient struct {
	flowv1.WorkflowServiceClient
	completed []*flowv1.RespondWorkflowTaskCompletedRequest
}

func (f *signalFakeClient) RespondWorkflowTaskCompleted(_ context.Context, req *flowv1.RespondWorkflowTaskCompletedRequest, _ ...grpc.CallOption) (*flowv1.RespondWorkflowTaskCompletedResponse, error) {
	f.completed = append(f.completed, req)
	return &flowv1.RespondWorkflowTaskCompletedResponse{}, nil
}

// TestProcessWorkflowTaskDeliversSignalOnStickyResume drives one run
// through three real processWorkflowTask calls — task 1 (cache miss:
// registers the handler, starts a timer, gets cached), task 2 (cache hit:
// a WorkflowExecutionSignaled event arrives via LoadNewEvents, delivered
// by the internal pump coroutine while the main coroutine is still parked
// waiting on its timer), task 3 (cache hit: the timer fires, the main
// coroutine wakes, observes the signal's already-recorded effect, and
// completes) — proving signal dispatch end to end through the real
// registration/dispatch path, not just Execution's own unit tests.
func TestProcessWorkflowTaskDeliversSignalOnStickyResume(t *testing.T) {
	signalTestPaymentID.Store("")

	exec := flowv1.WorkflowExecution{WorkflowId: "wf-signal-1", RunId: "run-1"}
	t1 := timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	t2 := timestamppb.New(time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC))
	t3 := timestamppb.New(time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC))

	task1History := []*flowv1.HistoryEvent{
		{EventId: 1, Attributes: &flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &flowv1.WorkflowExecutionStartedEventAttributes{
				WorkflowType: "signalTestWorkflow", Input: jsonPayload(t, struct{}{}),
			},
		}},
		{EventId: 2, Attributes: &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &flowv1.WorkflowTaskScheduledEventAttributes{},
		}},
		{EventId: 3, EventTime: t1, Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
		}},
	}

	// task2History: task1's history, plus the server durably recording the
	// signal (SignalWorkflowExecution's own two-event shape — see
	// history.SignalWorkflowExecution) and dispatching a new workflow task
	// for it. The timer task1 started is still open — nothing about it
	// appears here yet.
	task2History := append(
		append([]*flowv1.HistoryEvent{}, task1History...),
		&flowv1.HistoryEvent{EventId: 4, Attributes: &flowv1.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &flowv1.WorkflowTaskCompletedEventAttributes{},
		}},
		&flowv1.HistoryEvent{EventId: 5, Attributes: &flowv1.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &flowv1.TimerStartedEventAttributes{TimerId: 1, Duration: durationpb.New(time.Second)},
		}},
		&flowv1.HistoryEvent{EventId: 6, Attributes: &flowv1.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &flowv1.WorkflowExecutionSignaledEventAttributes{
				SignalName: "payment", Input: jsonPayload(t, "pay-42"),
			},
		}},
		&flowv1.HistoryEvent{EventId: 7, Attributes: &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &flowv1.WorkflowTaskScheduledEventAttributes{},
		}},
		&flowv1.HistoryEvent{EventId: 8, EventTime: t2, Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
		}},
	)

	// task3History: task2's history, plus the timer actually firing —
	// what finally wakes the main coroutine's Sleep to re-check received,
	// which the signal already set back in task 2.
	task3History := append(
		append([]*flowv1.HistoryEvent{}, task2History...),
		&flowv1.HistoryEvent{EventId: 9, Attributes: &flowv1.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &flowv1.WorkflowTaskCompletedEventAttributes{},
		}},
		&flowv1.HistoryEvent{EventId: 10, Attributes: &flowv1.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &flowv1.TimerFiredEventAttributes{TimerId: 1},
		}},
		&flowv1.HistoryEvent{EventId: 11, Attributes: &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &flowv1.WorkflowTaskScheduledEventAttributes{},
		}},
		&flowv1.HistoryEvent{EventId: 12, EventTime: t3, Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
		}},
	)

	fake := &signalFakeClient{}
	w := &Worker{
		rpc:                          fake,
		identity:                     "worker-under-test",
		workflows:                    make(map[string]workflowFunc),
		activities:                   make(map[string]activityFunc),
		execCache:                    newExecutionCache(defaultMaxCachedWorkflowExecutions),
		stickyScheduleToStartTimeout: 5 * time.Second,
		logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	w.RegisterWorkflow(signalTestWorkflow)

	ctx := context.Background()

	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("task-1"), WorkflowExecution: &exec, WorkflowType: "signalTestWorkflow", History: task1History,
	})
	if got := w.execCache.len(); got != 1 {
		t.Fatalf("after task 1, cache len = %d, want 1 (still running, waiting on its timer)", got)
	}
	if got := signalTestPaymentID.Load().(string); got != "" {
		t.Fatalf("payment ID observed before any signal arrived: %q", got)
	}

	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("task-2"), WorkflowExecution: &exec, WorkflowType: "signalTestWorkflow", History: task2History,
	})
	if got := signalTestPaymentID.Load().(string); got != "pay-42" {
		t.Fatalf("payment ID after task 2 = %q, want %q (the pump coroutine should have delivered the signal)", got, "pay-42")
	}
	if got := w.execCache.len(); got != 1 {
		t.Fatalf("after task 2, cache len = %d, want 1 (still running — the main coroutine hasn't noticed yet, only the handler ran)", got)
	}
	if len(fake.completed) != 2 || len(fake.completed[1].Commands) != 0 {
		t.Fatalf("task 2 commands = %+v, want zero (no new activity/timer scheduled, just an in-memory handler side effect)", fake.completed[1].Commands)
	}

	w.processWorkflowTask(ctx, &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("task-3"), WorkflowExecution: &exec, WorkflowType: "signalTestWorkflow", History: task3History,
	})
	if got := w.execCache.len(); got != 0 {
		t.Fatalf("after task 3 (terminal), cache len = %d, want 0", got)
	}
	if len(fake.completed) != 3 {
		t.Fatalf("expected 3 RespondWorkflowTaskCompleted calls total, got %d", len(fake.completed))
	}
	resp3 := fake.completed[2]
	if len(resp3.Commands) != 1 {
		t.Fatalf("task 3 commands = %v, want exactly 1 (CompleteWorkflowExecution)", resp3.Commands)
	}
	complete, ok := resp3.Commands[0].Command.(*flowv1.Command_CompleteWorkflowExecution)
	if !ok {
		t.Fatalf("task 3 command is %T, want CompleteWorkflowExecution", resp3.Commands[0].Command)
	}
	var result string
	if err := json.Unmarshal(complete.CompleteWorkflowExecution.Result.Data, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result != "pay-42" {
		t.Fatalf("result = %q, want %q", result, "pay-42")
	}
}
