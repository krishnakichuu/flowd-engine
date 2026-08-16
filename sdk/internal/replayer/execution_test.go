package replayer

import (
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionContinueAsNewRecordsCommand(t *testing.T) {
	e := NewExecution()
	input := &flowv1.Payload{Data: []byte(`"next"`)}

	e.ContinueAsNew("LoopWorkflow", input, ContinueAsNewOptions{
		TaskQueue:           "q2",
		WorkflowRunTimeout:  5 * time.Minute,
		WorkflowTaskTimeout: 10 * time.Second,
	})

	if e.Result != nil || e.Err != nil {
		t.Fatalf("ContinueAsNew must not set Result/Err (that's Complete's job), got Result=%v Err=%v", e.Result, e.Err)
	}
	if e.ContinuedAsNew == nil {
		t.Fatal("ContinuedAsNew was not set")
	}
	if got := e.ContinuedAsNew.WorkflowType; got != "LoopWorkflow" {
		t.Fatalf("WorkflowType = %q, want %q", got, "LoopWorkflow")
	}
	if e.ContinuedAsNew.Input != input {
		t.Fatal("Input was not carried through to the command")
	}
	if got := e.ContinuedAsNew.TaskQueue; got != "q2" {
		t.Fatalf("TaskQueue = %q, want %q", got, "q2")
	}
	if got := e.ContinuedAsNew.WorkflowRunTimeout.AsDuration(); got != 5*time.Minute {
		t.Fatalf("WorkflowRunTimeout = %v, want %v", got, 5*time.Minute)
	}
	if got := e.ContinuedAsNew.WorkflowTaskTimeout.AsDuration(); got != 10*time.Second {
		t.Fatalf("WorkflowTaskTimeout = %v, want %v", got, 10*time.Second)
	}

	if len(e.NewCommands) != 1 {
		t.Fatalf("NewCommands has %d entries, want exactly 1", len(e.NewCommands))
	}
	cmd, ok := e.NewCommands[0].Command.(*flowv1.Command_ContinueAsNewWorkflowExecution)
	if !ok {
		t.Fatalf("NewCommands[0] is %T, want *flowv1.Command_ContinueAsNewWorkflowExecution", e.NewCommands[0].Command)
	}
	if cmd.ContinueAsNewWorkflowExecution != e.ContinuedAsNew {
		t.Fatal("the command in NewCommands and Execution.ContinuedAsNew should be the same value")
	}
}

func TestExecutionContinueAsNewZeroOptionsLeavesFieldsUnset(t *testing.T) {
	e := NewExecution()
	e.ContinueAsNew("SameEverything", nil, ContinueAsNewOptions{})

	if e.ContinuedAsNew.TaskQueue != "" {
		t.Fatalf("TaskQueue = %q, want empty (unset means \"same as current run\")", e.ContinuedAsNew.TaskQueue)
	}
	if e.ContinuedAsNew.WorkflowRunTimeout != nil {
		t.Fatal("WorkflowRunTimeout should be nil when the option is zero, not a zero-value duration")
	}
	if e.ContinuedAsNew.WorkflowTaskTimeout != nil {
		t.Fatal("WorkflowTaskTimeout should be nil when the option is zero, not a zero-value duration")
	}
}

// TestLoadNewEventsUpdatesOutcomesAndClock simulates the sticky-cache-hit
// path: activity 1 and timer 1 were already scheduled in an earlier round
// (so a resumed coroutine's Future.Get calls are already blocked on them),
// and this round's new events are just their outcomes plus the new task's
// start time.
func TestLoadNewEventsUpdatesOutcomesAndClock(t *testing.T) {
	e := NewExecution()
	e.scheduledActivities = []scheduledActivity{{ActivityID: 1, ActivityType: "Foo"}}
	e.scheduledTimers = []scheduledTimer{{TimerID: 1, Duration: time.Second}}

	startTime := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	result := &flowv1.Payload{Data: []byte(`"ok"`)}

	e.LoadNewEvents([]*flowv1.HistoryEvent{
		{
			EventTime: timestamppb.New(startTime),
			Attributes: &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &flowv1.WorkflowTaskStartedEventAttributes{},
			},
		},
		{
			Attributes: &flowv1.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &flowv1.ActivityTaskCompletedEventAttributes{ActivityId: 1, Result: result},
			},
		},
		{
			Attributes: &flowv1.HistoryEvent_TimerFiredEventAttributes{
				TimerFiredEventAttributes: &flowv1.TimerFiredEventAttributes{TimerId: 1},
			},
		},
	})

	if !e.Now.Equal(startTime) {
		t.Fatalf("Now = %v, want %v", e.Now, startTime)
	}
	outcome, ok := e.activityOutcomes[1]
	if !ok || outcome.Result != result {
		t.Fatalf("activityOutcomes[1] = %+v, ok=%v, want Result=%v", outcome, ok, result)
	}
	if !e.firedTimers[1] {
		t.Fatal("firedTimers[1] should be true after a TimerFired event")
	}
}

// TestLoadNewEventsIgnoresScheduledEvents proves LoadNewEvents deliberately
// does less than LoadHistory: a resumed coroutine cannot re-execute code it
// already ran past, so it will never re-call ScheduleActivity/ScheduleTimer
// for something already recorded — there is nothing for LoadNewEvents to
// extend scheduledActivities/scheduledTimers with, and it doesn't try.
func TestLoadNewEventsIgnoresScheduledEvents(t *testing.T) {
	e := NewExecution()
	e.LoadNewEvents([]*flowv1.HistoryEvent{
		{
			Attributes: &flowv1.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &flowv1.ActivityTaskScheduledEventAttributes{ActivityId: 1, ActivityType: "Foo"},
			},
		},
		{
			Attributes: &flowv1.HistoryEvent_TimerStartedEventAttributes{
				TimerStartedEventAttributes: &flowv1.TimerStartedEventAttributes{TimerId: 1},
			},
		},
	})
	if len(e.scheduledActivities) != 0 {
		t.Fatalf("scheduledActivities = %v, want empty — LoadNewEvents must not populate it", e.scheduledActivities)
	}
	if len(e.scheduledTimers) != 0 {
		t.Fatalf("scheduledTimers = %v, want empty — LoadNewEvents must not populate it", e.scheduledTimers)
	}
}

func TestResetRoundOutputClearsPriorRoundOutput(t *testing.T) {
	e := NewExecution()
	e.Complete(&flowv1.Payload{Data: []byte(`"done"`)}, nil)

	if e.Result == nil || len(e.NewCommands) == 0 {
		t.Fatal("test setup: Complete should have set Result and NewCommands")
	}

	e.ResetRoundOutput()

	if e.NewCommands != nil {
		t.Fatalf("NewCommands = %v, want nil after ResetRoundOutput", e.NewCommands)
	}
	if e.Result != nil {
		t.Fatalf("Result = %v, want nil after ResetRoundOutput", e.Result)
	}
	if e.Err != nil {
		t.Fatalf("Err = %v, want nil after ResetRoundOutput", e.Err)
	}
	if e.ContinuedAsNew != nil {
		t.Fatalf("ContinuedAsNew = %v, want nil after ResetRoundOutput", e.ContinuedAsNew)
	}
}

func TestIsCancelRequestedFalseByDefault(t *testing.T) {
	e := NewExecution()
	if e.IsCancelRequested() {
		t.Fatal("IsCancelRequested should be false for a fresh Execution with no history loaded")
	}
	if got := e.CancelReason(); got != "" {
		t.Fatalf("CancelReason = %q, want empty when no cancellation was requested", got)
	}
}

func TestLoadHistorySetsCancelRequested(t *testing.T) {
	e := NewExecution()
	_, _ = e.LoadHistory([]*flowv1.HistoryEvent{
		{
			Attributes: &flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &flowv1.WorkflowExecutionStartedEventAttributes{WorkflowType: "Foo"},
			},
		},
		{
			Attributes: &flowv1.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
				WorkflowExecutionCancelRequestedEventAttributes: &flowv1.WorkflowExecutionCancelRequestedEventAttributes{Reason: "customer requested refund"},
			},
		},
	})

	if !e.IsCancelRequested() {
		t.Fatal("IsCancelRequested should be true after a WorkflowExecutionCancelRequested event")
	}
	if got := e.CancelReason(); got != "customer requested refund" {
		t.Fatalf("CancelReason = %q, want %q", got, "customer requested refund")
	}
}

// TestLoadNewEventsSetsCancelRequested proves a sticky-cached Execution
// (resumed via LoadNewEvents, not a fresh LoadHistory) also observes a
// cancel request delivered mid-flight — the same reasoning as why
// LoadNewEvents tracks ActivityTaskCompleted/TimerFired: a resumed
// coroutine still needs to see state that changed since it last ran.
func TestLoadNewEventsSetsCancelRequested(t *testing.T) {
	e := NewExecution()
	e.LoadNewEvents([]*flowv1.HistoryEvent{
		{
			Attributes: &flowv1.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
				WorkflowExecutionCancelRequestedEventAttributes: &flowv1.WorkflowExecutionCancelRequestedEventAttributes{Reason: "shutting down"},
			},
		},
	})

	if !e.IsCancelRequested() {
		t.Fatal("IsCancelRequested should be true after LoadNewEvents sees a WorkflowExecutionCancelRequested event")
	}
	if got := e.CancelReason(); got != "shutting down" {
		t.Fatalf("CancelReason = %q, want %q", got, "shutting down")
	}
}

func TestInvokeQueryHandlerNoneRegistered(t *testing.T) {
	e := NewExecution()
	_, err := e.InvokeQueryHandler("unknown", nil)
	if err == nil {
		t.Fatal("expected an error for an unregistered query type, got nil")
	}
}

func TestSetQueryHandlerAndInvoke(t *testing.T) {
	e := NewExecution()
	args := &flowv1.Payload{Data: []byte(`"ping"`)}
	want := &flowv1.Payload{Data: []byte(`"pong"`)}

	var gotArgs *flowv1.Payload
	e.SetQueryHandler("echo", func(a *flowv1.Payload) (*flowv1.Payload, error) {
		gotArgs = a
		return want, nil
	})

	got, err := e.InvokeQueryHandler("echo", args)
	if err != nil {
		t.Fatalf("InvokeQueryHandler: %v", err)
	}
	if got != want {
		t.Fatalf("result = %v, want the exact value the handler returned", got)
	}
	if gotArgs != args {
		t.Fatal("the handler did not receive the exact args passed to InvokeQueryHandler")
	}
}

func TestSetQueryHandlerLatestRegistrationWins(t *testing.T) {
	e := NewExecution()
	e.SetQueryHandler("q", func(*flowv1.Payload) (*flowv1.Payload, error) {
		return &flowv1.Payload{Data: []byte(`"first"`)}, nil
	})
	e.SetQueryHandler("q", func(*flowv1.Payload) (*flowv1.Payload, error) {
		return &flowv1.Payload{Data: []byte(`"second"`)}, nil
	})

	got, err := e.InvokeQueryHandler("q", nil)
	if err != nil {
		t.Fatalf("InvokeQueryHandler: %v", err)
	}
	if string(got.Data) != `"second"` {
		t.Fatalf("result = %s, want the most recently registered handler's answer", got.Data)
	}
}

func TestInvokeQueryHandlerPropagatesError(t *testing.T) {
	e := NewExecution()
	wantErr := errFixture{"handler failed"}
	e.SetQueryHandler("q", func(*flowv1.Payload) (*flowv1.Payload, error) {
		return nil, wantErr
	})
	_, err := e.InvokeQueryHandler("q", nil)
	if err != wantErr {
		t.Fatalf("got %v, want the exact error the handler returned", err)
	}
}

type errFixture struct{ msg string }

func (e errFixture) Error() string { return e.msg }

func signaledEvent(name string, payload *flowv1.Payload) *flowv1.HistoryEvent {
	return &flowv1.HistoryEvent{
		Attributes: &flowv1.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &flowv1.WorkflowExecutionSignaledEventAttributes{
				SignalName: name, Input: payload,
			},
		},
	}
}

// TestSetSignalHandlerDeliversBacklogFromLoadHistory proves a signal
// recorded in history before its handler is ever registered — the normal
// case on a full, non-sticky replay, where LoadHistory scans the entire
// run before workflow code has run at all — is still delivered: buffered
// as a pendingSignal, then flushed the instant SetSignalHandler registers
// a matching handler.
func TestSetSignalHandlerDeliversBacklogFromLoadHistory(t *testing.T) {
	e := NewExecution()
	want := &flowv1.Payload{Data: []byte(`"paid"`)}
	_, _ = e.LoadHistory([]*flowv1.HistoryEvent{signaledEvent("payment", want)})

	var got *flowv1.Payload
	e.SetSignalHandler("payment", func(a *flowv1.Payload) { got = a })

	if got != want {
		t.Fatalf("handler received %v, want the exact backlog payload %v", got, want)
	}
	if len(e.pendingSignals) != 0 {
		t.Fatalf("pendingSignals should be empty after delivery, got %d entries", len(e.pendingSignals))
	}
}

// TestSetSignalHandlerDeliversBacklogInOrder proves multiple backlogged
// signals of the same name are delivered in the order they were recorded.
func TestSetSignalHandlerDeliversBacklogInOrder(t *testing.T) {
	e := NewExecution()
	_, _ = e.LoadHistory([]*flowv1.HistoryEvent{
		signaledEvent("tick", &flowv1.Payload{Data: []byte(`1`)}),
		signaledEvent("tick", &flowv1.Payload{Data: []byte(`2`)}),
		signaledEvent("tick", &flowv1.Payload{Data: []byte(`3`)}),
	})

	var got []string
	e.SetSignalHandler("tick", func(a *flowv1.Payload) { got = append(got, string(a.Data)) })

	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered %v, want %v", got, want)
		}
	}
}

// TestUnregisteredSignalStaysPendingHarmlessly proves a signal_name this
// workflow declares no handler for is simply never delivered — durably
// recorded, observably inert — matching the pre-signal-dispatch behavior
// SignalMethod's Java counterpart documents for an unregistered signal.
func TestUnregisteredSignalStaysPendingHarmlessly(t *testing.T) {
	e := NewExecution()
	_, _ = e.LoadHistory([]*flowv1.HistoryEvent{signaledEvent("unhandled", nil)})

	e.SetSignalHandler("other", func(*flowv1.Payload) {
		t.Fatal("handler for a different signal_name must not be invoked")
	})

	if len(e.pendingSignals) != 1 {
		t.Fatalf("pendingSignals = %d entries, want the unhandled signal still queued", len(e.pendingSignals))
	}
}

// TestSetSignalHandlerLatestRegistrationWins mirrors
// TestSetQueryHandlerLatestRegistrationWins: re-registering the same
// signal_name replaces the handler, same map-assignment semantics as
// queries.
func TestSetSignalHandlerLatestRegistrationWins(t *testing.T) {
	e := NewExecution()
	var got string
	e.SetSignalHandler("q", func(*flowv1.Payload) { got = "first" })
	e.SetSignalHandler("q", func(*flowv1.Payload) { got = "second" })
	_, _ = e.LoadHistory([]*flowv1.HistoryEvent{signaledEvent("q", nil)})
	e.drainSignals()

	if got != "second" {
		t.Fatalf("got %q, want the most recently registered handler to have run", got)
	}
}

// TestSignalDeliveredOnLaterTaskViaLoadNewEvents proves the sticky-resume
// path: a handler registered in an earlier round (the common case — the
// coroutine that calls SetSignalHandler is long past that point, parked
// in Yield, and will never call it again) still receives a signal that
// arrives via LoadNewEvents on a later task, delivered by the internal
// pump coroutine SetSignalHandler starts.
func TestSignalDeliveredOnLaterTaskViaLoadNewEvents(t *testing.T) {
	e := NewExecution()
	var got *flowv1.Payload
	e.Dispatcher.Go(func(c *Coroutine) {
		e.SetSignalHandler("payment", func(a *flowv1.Payload) { got = a })
		for {
			c.Yield()
		}
	})
	e.Dispatcher.ExecuteRound() // round 1: registers the handler, no backlog yet

	if got != nil {
		t.Fatalf("got %v before any signal arrived, want nil", got)
	}

	want := &flowv1.Payload{Data: []byte(`"paid"`)}
	e.LoadNewEvents([]*flowv1.HistoryEvent{signaledEvent("payment", want)})
	e.Dispatcher.ExecuteRound() // round 2: the pump coroutine, now present, delivers it

	if got != want {
		t.Fatalf("got %v, want the exact payload %v delivered via LoadNewEvents", got, want)
	}
	if err := e.Dispatcher.FirstPanic(); err != nil {
		t.Fatalf("unexpected panic: %v", err)
	}
}

// TestSignalHandlerPanicSurfacesViaFirstPanic proves a delivery panic
// (e.g. a malformed payload in workflow.SetSignalHandler's reflection
// shim) is caught by the same Dispatcher.FirstPanic mechanism every other
// workflow panic goes through, whether delivered as backlog (inline,
// inside the registering coroutine) or later via the pump coroutine.
func TestSignalHandlerPanicSurfacesViaFirstPanic(t *testing.T) {
	e := NewExecution()
	e.Dispatcher.Go(func(c *Coroutine) {
		e.SetSignalHandler("boom", func(*flowv1.Payload) { panic("bad payload") })
		for {
			c.Yield()
		}
	})
	e.Dispatcher.ExecuteRound()

	e.LoadNewEvents([]*flowv1.HistoryEvent{signaledEvent("boom", nil)})
	e.Dispatcher.ExecuteRound()

	err := e.Dispatcher.FirstPanic()
	if err == nil {
		t.Fatal("expected FirstPanic to report the signal handler's panic")
	}
}
