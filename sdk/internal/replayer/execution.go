package replayer

import (
	"fmt"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NonDeterministicError is raised when replaying history against the
// current workflow code would schedule a different activity/timer than
// what history recorded at the same position — e.g. the workflow code
// changed in a way that isn't compatible with in-flight executions (see
// ADR-0001). It is a panic value caught by the coroutine's recover, so it
// surfaces via Dispatcher.FirstPanic and can be distinguished from an
// ordinary workflow panic with errors.As.
type NonDeterministicError struct{ Message string }

func (e *NonDeterministicError) Error() string { return "non-deterministic workflow: " + e.Message }

type ActivityOutcome struct {
	Result  *flowv1.Payload
	Failure *flowv1.Failure
}

type ActivityOptions struct {
	RetryPolicy            *flowv1.RetryPolicy
	ScheduleToStartTimeout time.Duration
	StartToCloseTimeout    time.Duration
}

type scheduledActivity struct {
	ActivityID   int64
	ActivityType string
}

type scheduledTimer struct {
	TimerID  int64
	Duration time.Duration
}

// Execution is the per-workflow-task state shared by all of a workflow's
// coroutines. It separates what history already recorded (already-scheduled
// activities/timers and their outcomes) from what workflow code newly
// schedules during this task's single round (see Run) — only the latter
// becomes a Command sent back to the server.
type Execution struct {
	Dispatcher *Dispatcher

	Now time.Time

	scheduledActivities []scheduledActivity
	scheduledTimers     []scheduledTimer
	activityOutcomes    map[int64]ActivityOutcome
	firedTimers         map[int64]bool

	nextActivityIdx int64
	nextTimerIdx    int64

	NewCommands []*flowv1.Command

	Result *flowv1.Payload
	Err    *flowv1.Failure

	// ContinuedAsNew is set instead of Result/Err when the workflow function
	// asked to continue as new rather than complete or fail — see
	// ContinueAsNew. A caller (e.g. Worker.processWorkflowTask,
	// ReplayWorkflowHistory) checks this to distinguish "returned normally"
	// from "asked for a fresh run" once the round is done.
	ContinuedAsNew *flowv1.ContinueAsNewWorkflowExecutionCommand

	// cancelRequested/cancelReason are set once history records a
	// WorkflowExecutionCancelRequestedEventAttributes event (from a
	// RequestCancelWorkflowExecution call) and never cleared — a
	// cancellation request, once made, stays true for the rest of this
	// run, the same "sticky until this run ends" semantics as a fired
	// timer, not a one-shot signal. Workflow code observes it via
	// workflow.IsCancelRequested/CancelReason and decides for itself how
	// to end (see cancel.go, Phase 2 roadmap, Track B, item 3).
	cancelRequested bool
	cancelReason    string

	queryHandlers map[string]QueryHandler

	signalHandlers    map[string]SignalHandler
	pendingSignals    []pendingSignal
	signalPumpStarted bool
}

// IsCancelRequested reports whether a RequestCancelWorkflowExecution call
// has been recorded against this run — see workflow.IsCancelRequested,
// the workflow-author-facing wrapper.
func (e *Execution) IsCancelRequested() bool {
	return e.cancelRequested
}

// CancelReason returns the reason string from the most recent cancel
// request, or "" if none has been requested (check IsCancelRequested
// first — a genuinely empty reason is indistinguishable from "no request"
// otherwise).
func (e *Execution) CancelReason() string {
	return e.cancelReason
}

// QueryHandler answers one query type against the workflow's current
// in-memory state. It's invoked directly (see InvokeQueryHandler), not
// through the coroutine's own Yield/resume cycle, so it must be a pure,
// synchronous read of already-captured workflow state — no blocking
// primitive (ExecuteActivity, Sleep) is safe to call from inside one.
type QueryHandler func(args *flowv1.Payload) (*flowv1.Payload, error)

// SetQueryHandler registers h to answer queries of queryType — see
// workflow.SetQueryHandler, the workflow-author-facing wrapper. Typically
// called once, near the top of a workflow function, before its first
// blocking primitive: registering it there guarantees a single
// ExecuteRound has already run it by the time any query can arrive, cache
// hit or miss alike.
func (e *Execution) SetQueryHandler(queryType string, h QueryHandler) {
	if e.queryHandlers == nil {
		e.queryHandlers = make(map[string]QueryHandler)
	}
	e.queryHandlers[queryType] = h
}

// InvokeQueryHandler answers a query using whatever handler is currently
// registered for queryType. Safe to call between tasks/rounds: the
// coroutine that registered the handler is parked in Yield, not
// concurrently running, so reading whatever state its closure captured
// cannot race with it.
func (e *Execution) InvokeQueryHandler(queryType string, args *flowv1.Payload) (*flowv1.Payload, error) {
	h, ok := e.queryHandlers[queryType]
	if !ok {
		return nil, fmt.Errorf("no query handler registered for %q", queryType)
	}
	return h(args)
}

// SignalHandler acts on a signal's decoded payload — see
// workflow.SetSignalHandler, the workflow-author-facing wrapper. Unlike
// QueryHandler, it returns nothing: SignalWorkflowExecution is
// asynchronous and fire-and-forget by design (see client.Client.SignalWorkflow,
// which does not wait for or expose one), so there is no reply channel for
// it to answer through.
type SignalHandler func(args *flowv1.Payload)

// pendingSignal is a WorkflowExecutionSignaled event observed by
// LoadHistory/LoadNewEvents that has no registered handler yet (or didn't
// at the moment it was observed) — see Execution.drainSignals.
type pendingSignal struct {
	name    string
	payload *flowv1.Payload
}

// SetSignalHandler registers h to act on signals of signalName, delivered
// from a WorkflowExecutionSignaled history event (Phase 2 roadmap item:
// signal dispatch) — see workflow.SetSignalHandler, the
// workflow-author-facing wrapper. Like SetQueryHandler, typically called
// once, near the top of a workflow function, before its first blocking
// call.
//
// Unlike a query, a signal can already be sitting in history before its
// handler is ever registered — a full, non-sticky replay scans a run's
// entire history, including every signal it ever recorded, before
// workflow code runs at all (LoadHistory) — so any such backlog
// (Execution.pendingSignals) is delivered right here, synchronously, in
// the order recorded, the instant a matching handler registers.
//
// A signal recorded on a *later* task, once the handler is already
// registered, is delivered by a dedicated internal coroutine started
// (lazily, once per Execution) below, not directly from LoadNewEvents:
// LoadNewEvents runs outside any coroutine (see worker.go's cache-hit
// path, which calls it before Dispatcher.ExecuteRound), so it has no
// recover() to safely catch a delivery panic (e.g. a malformed payload
// that fails to unmarshal in workflow.SetSignalHandler's reflection
// shim). Routing every delivery through a real coroutine instead means
// such a panic is caught by the exact same Dispatcher.FirstPanic()
// mechanism every other workflow panic already goes through, cached
// state and all (see worker.go's post-ExecuteRound panic handling).
func (e *Execution) SetSignalHandler(signalName string, h SignalHandler) {
	if e.signalHandlers == nil {
		e.signalHandlers = make(map[string]SignalHandler)
	}
	e.signalHandlers[signalName] = h
	e.drainSignals()

	if e.signalPumpStarted {
		return
	}
	e.signalPumpStarted = true
	e.Dispatcher.Go(func(c *Coroutine) {
		for {
			e.drainSignals()
			c.Yield()
		}
	})
}

// drainSignals delivers every currently-pending signal whose handler is
// now registered, in the order recorded, leaving anything still unclaimed
// (no handler registered for that signal_name yet — or ever, for a
// signal_name this workflow simply never handles) queued for later.
func (e *Execution) drainSignals() {
	if len(e.pendingSignals) == 0 {
		return
	}
	var remaining []pendingSignal
	for _, sig := range e.pendingSignals {
		if h, ok := e.signalHandlers[sig.name]; ok {
			h(sig.payload)
		} else {
			remaining = append(remaining, sig)
		}
	}
	e.pendingSignals = remaining
}

func NewExecution() *Execution {
	return &Execution{
		Dispatcher:       NewDispatcher(),
		activityOutcomes: make(map[int64]ActivityOutcome),
		firedTimers:      make(map[int64]bool),
	}
}

// LoadHistory scans a workflow run's full history from the beginning and
// returns the original start input/workflow type, populating the
// already-scheduled activity/timer state used by
// ScheduleActivity/ScheduleTimer to detect non-determinism and avoid
// re-emitting commands for already-recorded work. Used for a fresh
// Execution — either genuinely the first task, or a cache-miss fallback
// (see LoadNewEvents for the sticky-cache-hit alternative).
func (e *Execution) LoadHistory(events []*flowv1.HistoryEvent) (input *flowv1.Payload, workflowType string) {
	for _, ev := range events {
		switch a := ev.Attributes.(type) {
		case *flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes:
			input = a.WorkflowExecutionStartedEventAttributes.Input
			workflowType = a.WorkflowExecutionStartedEventAttributes.WorkflowType
		case *flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes:
			e.Now = ev.EventTime.AsTime()
		case *flowv1.HistoryEvent_ActivityTaskScheduledEventAttributes:
			at := a.ActivityTaskScheduledEventAttributes
			e.scheduledActivities = append(e.scheduledActivities, scheduledActivity{ActivityID: at.ActivityId, ActivityType: at.ActivityType})
		case *flowv1.HistoryEvent_ActivityTaskCompletedEventAttributes:
			ac := a.ActivityTaskCompletedEventAttributes
			e.activityOutcomes[ac.ActivityId] = ActivityOutcome{Result: ac.Result}
		case *flowv1.HistoryEvent_ActivityTaskFailedEventAttributes:
			af := a.ActivityTaskFailedEventAttributes
			e.activityOutcomes[af.ActivityId] = ActivityOutcome{Failure: af.Failure}
		case *flowv1.HistoryEvent_TimerStartedEventAttributes:
			ts := a.TimerStartedEventAttributes
			e.scheduledTimers = append(e.scheduledTimers, scheduledTimer{TimerID: ts.TimerId, Duration: ts.Duration.AsDuration()})
		case *flowv1.HistoryEvent_TimerFiredEventAttributes:
			e.firedTimers[a.TimerFiredEventAttributes.TimerId] = true
		case *flowv1.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes:
			e.cancelRequested = true
			e.cancelReason = a.WorkflowExecutionCancelRequestedEventAttributes.Reason
		case *flowv1.HistoryEvent_WorkflowExecutionSignaledEventAttributes:
			sig := a.WorkflowExecutionSignaledEventAttributes
			e.pendingSignals = append(e.pendingSignals, pendingSignal{name: sig.SignalName, payload: sig.Input})
		}
	}
	return input, workflowType
}

// LoadNewEvents feeds events a cached Execution hasn't seen yet into it, to
// resume a sticky task instead of a full LoadHistory + fresh coroutines
// (Phase 2 roadmap, Track C, item 1). It deliberately does far less than
// LoadHistory: a resumed coroutine is a real, already-running goroutine
// blocked in Yield — it physically cannot re-execute code it already ran
// past, so it will never call ScheduleActivity/ScheduleTimer again for
// something already in scheduledActivities/scheduledTimers, and those are
// left untouched here. Only the two things a blocked coroutine is actually
// waiting to observe are updated: an activity/timer outcome (unblocks
// ActivityFuture.Get/TimerFuture.Get on their next Yield-driven check) and
// the new task's start time.
func (e *Execution) LoadNewEvents(events []*flowv1.HistoryEvent) {
	for _, ev := range events {
		switch a := ev.Attributes.(type) {
		case *flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes:
			e.Now = ev.EventTime.AsTime()
		case *flowv1.HistoryEvent_ActivityTaskCompletedEventAttributes:
			ac := a.ActivityTaskCompletedEventAttributes
			e.activityOutcomes[ac.ActivityId] = ActivityOutcome{Result: ac.Result}
		case *flowv1.HistoryEvent_ActivityTaskFailedEventAttributes:
			af := a.ActivityTaskFailedEventAttributes
			e.activityOutcomes[af.ActivityId] = ActivityOutcome{Failure: af.Failure}
		case *flowv1.HistoryEvent_TimerFiredEventAttributes:
			e.firedTimers[a.TimerFiredEventAttributes.TimerId] = true
		case *flowv1.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes:
			e.cancelRequested = true
			e.cancelReason = a.WorkflowExecutionCancelRequestedEventAttributes.Reason
		case *flowv1.HistoryEvent_WorkflowExecutionSignaledEventAttributes:
			sig := a.WorkflowExecutionSignaledEventAttributes
			e.pendingSignals = append(e.pendingSignals, pendingSignal{name: sig.SignalName, payload: sig.Input})
		}
	}
}

// ResetRoundOutput clears the previous round's output before resuming a
// cached Execution for a new task. A sticky Execution's Dispatcher and
// coroutines persist in memory across tasks — that's the whole point — but
// NewCommands/Result/Err/ContinuedAsNew must not: without this, a later
// task would resend commands already reported to the server in an earlier
// response.
func (e *Execution) ResetRoundOutput() {
	e.NewCommands = nil
	e.Result = nil
	e.Err = nil
	e.ContinuedAsNew = nil
}

// ActivityFuture resolves once the activity it represents has a recorded
// outcome (from history, or from a later task once it completes).
type ActivityFuture struct {
	exec *Execution
	id   int64
}

// Get blocks (yielding the calling coroutine) until this activity's outcome
// is known. It only returns within the current round if the outcome was
// already present in history; otherwise the coroutine yields and this call
// does not return until a future task reloads history with the outcome
// present.
func (f *ActivityFuture) Get(c *Coroutine) (*flowv1.Payload, *flowv1.Failure) {
	for {
		if outcome, ok := f.exec.activityOutcomes[f.id]; ok {
			return outcome.Result, outcome.Failure
		}
		c.Yield()
	}
}

// ScheduleActivity is workflow.ExecuteActivity's engine-level primitive.
// Activity IDs are assigned purely by call order (not server-assigned),
// which is what makes them reproducible across replay: the Nth
// ScheduleActivity call always gets ID N+1, whether this is the original
// execution or a replay.
func (e *Execution) ScheduleActivity(activityType string, input *flowv1.Payload, opts ActivityOptions) *ActivityFuture {
	idx := e.nextActivityIdx
	e.nextActivityIdx++
	id := idx + 1

	if idx < int64(len(e.scheduledActivities)) {
		recorded := e.scheduledActivities[idx]
		if recorded.ActivityID != id || recorded.ActivityType != activityType {
			panic(&NonDeterministicError{Message: fmt.Sprintf(
				"activity #%d: workflow now schedules type %q but history recorded id=%d type %q",
				idx+1, activityType, recorded.ActivityID, recorded.ActivityType,
			)})
		}
		return &ActivityFuture{exec: e, id: id}
	}

	e.NewCommands = append(e.NewCommands, &flowv1.Command{Command: &flowv1.Command_ScheduleActivityTask{
		ScheduleActivityTask: &flowv1.ScheduleActivityTaskCommand{
			ActivityId:             id,
			ActivityType:           activityType,
			Input:                  input,
			RetryPolicy:            opts.RetryPolicy,
			ScheduleToStartTimeout: durationOrNil(opts.ScheduleToStartTimeout),
			StartToCloseTimeout:    durationOrNil(opts.StartToCloseTimeout),
		},
	}})
	return &ActivityFuture{exec: e, id: id}
}

// TimerFuture resolves once its timer has fired (per history, or a future
// task once the server-side timer firer records TimerFired).
type TimerFuture struct {
	exec *Execution
	id   int64
}

func (f *TimerFuture) Get(c *Coroutine) {
	for !f.exec.firedTimers[f.id] {
		c.Yield()
	}
}

// ScheduleTimer is workflow.Sleep/NewTimer's engine-level primitive, same
// call-order ID assignment and non-determinism check as ScheduleActivity.
func (e *Execution) ScheduleTimer(d time.Duration) *TimerFuture {
	idx := e.nextTimerIdx
	e.nextTimerIdx++
	id := idx + 1

	if idx < int64(len(e.scheduledTimers)) {
		recorded := e.scheduledTimers[idx]
		if recorded.TimerID != id {
			panic(&NonDeterministicError{Message: fmt.Sprintf(
				"timer #%d: workflow now starts a timer but history recorded a different timer id=%d",
				idx+1, recorded.TimerID,
			)})
		}
		return &TimerFuture{exec: e, id: id}
	}

	e.NewCommands = append(e.NewCommands, &flowv1.Command{Command: &flowv1.Command_StartTimer{
		StartTimer: &flowv1.StartTimerCommand{TimerId: id, Duration: durationpb.New(d)},
	}})
	return &TimerFuture{exec: e, id: id}
}

// Complete records the workflow's terminal outcome as a command; called
// once the workflow function returns within Run's single round.
func (e *Execution) Complete(result *flowv1.Payload, failure *flowv1.Failure) {
	if failure != nil {
		e.Err = failure
		e.NewCommands = append(e.NewCommands, &flowv1.Command{Command: &flowv1.Command_FailWorkflowExecution{
			FailWorkflowExecution: &flowv1.FailWorkflowExecutionCommand{Failure: failure},
		}})
		return
	}
	e.Result = result
	e.NewCommands = append(e.NewCommands, &flowv1.Command{Command: &flowv1.Command_CompleteWorkflowExecution{
		CompleteWorkflowExecution: &flowv1.CompleteWorkflowExecutionCommand{Result: result},
	}})
}

// ContinueAsNewOptions configures a ContinueAsNew call. A zero value means
// "same as the current run" for every field (see
// flow.api.v1.ContinueAsNewWorkflowExecutionCommand's doc).
type ContinueAsNewOptions struct {
	TaskQueue           string
	RetryPolicy         *flowv1.RetryPolicy
	WorkflowRunTimeout  time.Duration
	WorkflowTaskTimeout time.Duration
}

// ContinueAsNew records a continue-as-new command in place of Complete:
// this run closes and a fresh run of workflowType starts under the same
// workflow_id (see Phase 2 roadmap, Track B, item 2). Like Complete, this
// is called once the workflow function returns within Run's single round —
// the caller (sdk/worker) is responsible for detecting the
// workflow.ContinueAsNewError sentinel and routing here instead of
// Complete, since Execution has no visibility into workflow-level error
// types.
func (e *Execution) ContinueAsNew(workflowType string, input *flowv1.Payload, opts ContinueAsNewOptions) {
	cmd := &flowv1.ContinueAsNewWorkflowExecutionCommand{
		WorkflowType:        workflowType,
		TaskQueue:           opts.TaskQueue,
		Input:               input,
		RetryPolicy:         opts.RetryPolicy,
		WorkflowRunTimeout:  durationOrNil(opts.WorkflowRunTimeout),
		WorkflowTaskTimeout: durationOrNil(opts.WorkflowTaskTimeout),
	}
	e.ContinuedAsNew = cmd
	e.NewCommands = append(e.NewCommands, &flowv1.Command{Command: &flowv1.Command_ContinueAsNewWorkflowExecution{
		ContinueAsNewWorkflowExecution: cmd,
	}})
}

func durationOrNil(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}
	return durationpb.New(d)
}
