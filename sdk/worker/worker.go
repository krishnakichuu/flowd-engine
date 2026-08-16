// Package worker implements the Worker type: it registers workflow and
// activity functions, long-polls both task queues, and drives workflow
// tasks through the deterministic replayer (sdk/internal/replayer).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/activity"
	"github.com/krishnakichuu/flowd/sdk/client"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
	"github.com/krishnakichuu/flowd/sdk/internal/replayer"
	"google.golang.org/protobuf/types/known/durationpb"
)

// pollRetryBackoff is how long to wait after a failed poll RPC (e.g. the
// server is temporarily unreachable) before retrying.
const pollRetryBackoff = time.Second

// defaultMaxConcurrentActivities bounds how many activity tasks a single
// worker processes at once, used when Options.MaxConcurrentActivities is
// left at zero. Without a bound, a burst of ready activity tasks spawns
// one goroutine per task with no limit — a real resource-exhaustion risk,
// not a hypothetical (see the Phase 2 roadmap, Track B, item 1). The Java
// SDK port carries the same default.
const defaultMaxConcurrentActivities = 200

// defaultMaxCachedWorkflowExecutions bounds how many workflow Executions
// this worker keeps cached in memory between tasks — sticky caching, used
// when Options.MaxCachedWorkflowExecutions is left at zero (see
// processWorkflowTask and the Phase 2 roadmap, Track C, item 1).
const defaultMaxCachedWorkflowExecutions = 1000

// defaultStickyScheduleToStartTimeout bounds how long the server prefers
// routing a cached run's next workflow task back to this worker before
// falling back to any available worker, used when
// Options.StickyScheduleToStartTimeout is left at zero.
const defaultStickyScheduleToStartTimeout = 5 * time.Second

type Options struct {
	Logger *slog.Logger
	// MaxConcurrentActivities bounds how many activity tasks this worker
	// processes at once. Zero (the default) uses
	// defaultMaxConcurrentActivities.
	MaxConcurrentActivities int
	// MaxCachedWorkflowExecutions bounds this worker's sticky Execution
	// cache. A larger cache means more runs can stay sticky at once, at
	// the cost of one blocked goroutine per cached run (see
	// replayer.Coroutine.Yield's doc on what eviction past capacity
	// costs). Zero uses defaultMaxCachedWorkflowExecutions.
	MaxCachedWorkflowExecutions int
	// StickyScheduleToStartTimeout bounds how long a cached run's next
	// workflow task is preferentially routed back to this worker before
	// falling back to any available worker — which always triggers a full
	// replay, so sticky caching is a pure optimization, never a
	// correctness requirement. Zero uses
	// defaultStickyScheduleToStartTimeout.
	StickyScheduleToStartTimeout time.Duration
	// TaskQueuePartitions restricts this worker to the listed
	// task_queue_partition values — nil/empty (the default) means every
	// partition, Phase 1's original behavior (Phase 2 roadmap, Track C,
	// item 3). Meaningless unless the server is configured with more than
	// one partition (FLOWD_NUM_TASK_QUEUE_PARTITIONS); every task lives in
	// partition 0 otherwise, so a worker that declares a subset here would
	// simply never see anything.
	TaskQueuePartitions []int32
}

type Worker struct {
	rpc       flowv1.WorkflowServiceClient
	namespace string
	taskQueue string
	logger    *slog.Logger
	identity  string

	workflows  map[string]workflowFunc
	activities map[string]activityFunc

	// activitySem bounds concurrent processActivityTask goroutines. A
	// buffered channel used as a counting semaphore — standard Go idiom:
	// acquire by sending, release by receiving.
	activitySem chan struct{}

	// execCache backs sticky caching (see processWorkflowTask). It's
	// touched only from pollWorkflowTasks' single goroutine — workflow
	// tasks are processed one at a time per worker — so it needs no
	// locking.
	execCache                    *executionCache
	stickyScheduleToStartTimeout time.Duration
	taskQueuePartitions          []int32
}

func New(c *client.Client, taskQueue string, opts Options) *Worker {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxConcurrentActivities := opts.MaxConcurrentActivities
	if maxConcurrentActivities <= 0 {
		maxConcurrentActivities = defaultMaxConcurrentActivities
	}
	maxCachedWorkflowExecutions := opts.MaxCachedWorkflowExecutions
	if maxCachedWorkflowExecutions <= 0 {
		maxCachedWorkflowExecutions = defaultMaxCachedWorkflowExecutions
	}
	stickyScheduleToStartTimeout := opts.StickyScheduleToStartTimeout
	if stickyScheduleToStartTimeout <= 0 {
		stickyScheduleToStartTimeout = defaultStickyScheduleToStartTimeout
	}
	host, _ := os.Hostname()
	return &Worker{
		rpc:                          c.WorkflowService(),
		namespace:                    c.Namespace(),
		taskQueue:                    taskQueue,
		logger:                       logger,
		identity:                     fmt.Sprintf("%s:%d", host, os.Getpid()),
		workflows:                    make(map[string]workflowFunc),
		activities:                   make(map[string]activityFunc),
		activitySem:                  make(chan struct{}, maxConcurrentActivities),
		execCache:                    newExecutionCache(maxCachedWorkflowExecutions),
		stickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
		taskQueuePartitions:          opts.TaskQueuePartitions,
	}
}

// Run polls both task queues until ctx is canceled.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w.pollWorkflowTasks(ctx) }()
	go func() { defer wg.Done(); w.pollActivityTasks(ctx) }()
	wg.Wait()
}

func (w *Worker) pollWorkflowTasks(ctx context.Context) {
	for ctx.Err() == nil {
		resp, err := w.rpc.PollWorkflowTaskQueue(ctx, &flowv1.PollWorkflowTaskQueueRequest{
			Namespace: w.namespace, TaskQueue: w.taskQueue, Identity: w.identity,
			TaskQueuePartitions: w.taskQueuePartitions,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("poll workflow task queue", "error", err)
			time.Sleep(pollRetryBackoff)
			continue
		}
		if len(resp.TaskToken) == 0 {
			continue // long-poll timed out with nothing available
		}
		w.processWorkflowTask(ctx, resp)
	}
}

func (w *Worker) pollActivityTasks(ctx context.Context) {
	for ctx.Err() == nil {
		// Acquire a concurrency slot before polling, not after dispatch —
		// so this worker never holds a dispatched task's time-bounded
		// lease (ADR-0002) while merely waiting for processing capacity to
		// free up.
		select {
		case w.activitySem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		resp, err := w.rpc.PollActivityTaskQueue(ctx, &flowv1.PollActivityTaskQueueRequest{
			Namespace: w.namespace, TaskQueue: w.taskQueue, Identity: w.identity,
			TaskQueuePartitions: w.taskQueuePartitions,
		})
		if err != nil {
			<-w.activitySem
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("poll activity task queue", "error", err)
			time.Sleep(pollRetryBackoff)
			continue
		}
		if len(resp.TaskToken) == 0 {
			<-w.activitySem
			continue
		}
		go func() {
			defer func() { <-w.activitySem }()
			w.processActivityTask(ctx, resp)
		}()
	}
}

// processWorkflowTask advances the run one step. On a cache hit — this
// worker already has the run's Execution cached from a previous task, and
// the server routed this one back sticky — it resumes that Execution with
// only the events it hasn't seen yet, skipping a full replay entirely (see
// replayer.Execution.LoadNewEvents and Coroutine.Yield's doc for why a
// cached Execution's coroutines can resume exactly where they left off).
// On a miss — the first task, a cache eviction, or a different worker
// picked this run up — it replays the full history through a fresh
// Execution/Dispatcher, exactly as before sticky caching existed. Either
// way it reports the resulting commands — or a distinct non-determinism
// failure — back to the server.
func (w *Worker) processWorkflowTask(ctx context.Context, resp *flowv1.PollWorkflowTaskQueueResponse) {
	if resp.QueryTask != nil {
		w.processQueryTask(ctx, resp)
		return
	}

	key := execCacheKey{WorkflowID: resp.WorkflowExecution.WorkflowId, RunID: resp.WorkflowExecution.RunId}

	var exec *replayer.Execution
	var workflowType string

	if cached, hit := w.execCache.get(key); hit {
		exec = cached.exec
		workflowType = cached.workflowType
		exec.ResetRoundOutput()
		exec.LoadNewEvents(eventsAfter(resp.History, cached.lastEventID))
		exec.Dispatcher.ExecuteRound()
	} else {
		exec = replayer.NewExecution()
		var input *flowv1.Payload
		input, workflowType = exec.LoadHistory(resp.History)

		wf, ok := w.workflows[workflowType]
		if !ok {
			w.respondWorkflowTaskFailed(ctx, resp.TaskToken, fmt.Sprintf("unregistered workflow type %q", workflowType))
			return
		}

		if err := runWorkflowFunc(exec, wf.fn, wf.inputType, input); err != nil {
			w.respondWorkflowTaskFailed(ctx, resp.TaskToken, err.Error())
			return
		}
	}

	if panicErr := exec.Dispatcher.FirstPanic(); panicErr != nil {
		w.execCache.delete(key) // cached state, if any, is exactly what's suspect
		var ndErr *replayer.NonDeterministicError
		if errors.As(panicErr, &ndErr) {
			w.logger.Error("non-deterministic workflow detected", "workflow_type", workflowType, "error", ndErr)
		} else {
			w.logger.Error("workflow panic", "workflow_type", workflowType, "error", panicErr)
		}
		w.respondWorkflowTaskFailed(ctx, resp.TaskToken, panicErr.Error())
		return
	}

	var stickyAttrs *flowv1.StickyExecutionAttributes
	if exec.Result == nil && exec.Err == nil && exec.ContinuedAsNew == nil {
		// Still running: cache it and ask the server to prefer routing
		// this run's next task back to us.
		w.execCache.put(key, &cachedExecution{
			exec:         exec,
			workflowType: workflowType,
			lastEventID:  resp.History[len(resp.History)-1].EventId,
		})
		stickyAttrs = &flowv1.StickyExecutionAttributes{
			WorkerIdentity:         w.identity,
			ScheduleToStartTimeout: durationpb.New(w.stickyScheduleToStartTimeout),
		}
	} else {
		// Terminal, or continued into a different run_id: nothing more
		// will ever be dispatched for this run, so there's nothing to
		// keep cached.
		w.execCache.delete(key)
	}

	if _, err := w.rpc.RespondWorkflowTaskCompleted(ctx, &flowv1.RespondWorkflowTaskCompletedRequest{
		TaskToken: resp.TaskToken, Commands: exec.NewCommands, Identity: w.identity,
		StickyExecutionAttributes: stickyAttrs,
	}); err != nil {
		// The server rejected our response — most commonly
		// ErrConcurrentModification, when a client-initiated write
		// (Signal, RequestCancelWorkflowExecution) landed on this run
		// between when we loaded its history and now. Per
		// history.AppendHistory's own contract, a caller in this position
		// "must abort ... and discard any in-memory replay state": the
		// cache entry put above (if this run is still active) reflects
		// coroutines that believe commands we just tried to report
		// (timers started, activities scheduled, ...) were recorded —
		// they were not. Leaving it cached would let the *next* task
		// resume those same coroutines waiting on state the server never
		// actually created, a permanent deadlock (found live while
		// testing RequestCancelWorkflowExecution, whose CAS retry makes
		// this race far easier to hit than it was before, but the bug
		// predates it — any concurrent Signal could trigger the same
		// thing). Evicting forces the next task to fall back to a full
		// LoadHistory from the server's actual, authoritative state.
		w.execCache.delete(key)
		w.logger.Error("respond workflow task completed", "error", err)
	}
}

// eventsAfter returns the suffix of history with event_id > lastEventID —
// what a cache hit still needs to feed into its already-resumed Execution.
// The server always sends the run's full history on every dispatch (see
// internal/history's maxHistoryFetch doc); this is what lets the worker,
// which is the only side that reliably knows how much it's already
// processed, slice off just the new tail itself.
func eventsAfter(history []*flowv1.HistoryEvent, lastEventID int64) []*flowv1.HistoryEvent {
	for i, ev := range history {
		if ev.EventId > lastEventID {
			return history[i:]
		}
	}
	return nil
}

// processQueryTask answers a query dispatch (Phase 2 roadmap, Track D,
// item 1): resume the run's cached Execution if this worker has one, or
// rebuild it via a full replay if not, then invoke whatever handler the
// workflow registered for this query_type — entirely in memory, no
// commands, no RespondWorkflowTaskCompleted, nothing appended to history.
// A successful replay-to-rebuild is opportunistically cached too (subject
// to normal LRU eviction), same as a real workflow task cache miss, since
// a follow-up query or workflow task on this run benefits from it exactly
// the same way.
func (w *Worker) processQueryTask(ctx context.Context, resp *flowv1.PollWorkflowTaskQueueResponse) {
	key := execCacheKey{WorkflowID: resp.WorkflowExecution.WorkflowId, RunID: resp.WorkflowExecution.RunId}

	var exec *replayer.Execution
	if cached, hit := w.execCache.get(key); hit {
		exec = cached.exec
	} else {
		exec = replayer.NewExecution()
		input, workflowType := exec.LoadHistory(resp.History)

		wf, ok := w.workflows[workflowType]
		if !ok {
			w.respondQueryTaskFailed(ctx, resp.TaskToken, fmt.Sprintf("unregistered workflow type %q", workflowType))
			return
		}
		if err := runWorkflowFunc(exec, wf.fn, wf.inputType, input); err != nil {
			w.respondQueryTaskFailed(ctx, resp.TaskToken, err.Error())
			return
		}
		if panicErr := exec.Dispatcher.FirstPanic(); panicErr != nil {
			w.respondQueryTaskFailed(ctx, resp.TaskToken, panicErr.Error())
			return
		}
		if exec.Result == nil && exec.Err == nil && exec.ContinuedAsNew == nil {
			w.execCache.put(key, &cachedExecution{
				exec: exec, workflowType: workflowType,
				lastEventID: resp.History[len(resp.History)-1].EventId,
			})
		}
	}

	result, err := exec.InvokeQueryHandler(resp.QueryTask.QueryType, resp.QueryTask.QueryArgs)
	if err != nil {
		w.respondQueryTaskFailed(ctx, resp.TaskToken, err.Error())
		return
	}
	if _, err := w.rpc.RespondQueryTaskCompleted(ctx, &flowv1.RespondQueryTaskCompletedRequest{
		TaskToken: resp.TaskToken, Result: result,
	}); err != nil {
		w.logger.Error("respond query task completed", "error", err)
	}
}

func (w *Worker) respondQueryTaskFailed(ctx context.Context, token []byte, message string) {
	if _, err := w.rpc.RespondQueryTaskCompleted(ctx, &flowv1.RespondQueryTaskCompletedRequest{
		TaskToken: token, Failure: &flowv1.Failure{Message: message},
	}); err != nil {
		w.logger.Error("respond query task failed", "error", err)
	}
}

func (w *Worker) respondWorkflowTaskFailed(ctx context.Context, token []byte, message string) {
	if _, err := w.rpc.RespondWorkflowTaskFailed(ctx, &flowv1.RespondWorkflowTaskFailedRequest{
		TaskToken: token, Failure: &flowv1.Failure{Message: message}, Identity: w.identity,
	}); err != nil {
		w.logger.Error("respond workflow task failed", "error", err)
	}
}

func (w *Worker) processActivityTask(ctx context.Context, resp *flowv1.PollActivityTaskQueueResponse) {
	act, ok := w.activities[resp.ActivityType]
	if !ok {
		w.respondActivityTaskFailed(ctx, resp.TaskToken, "UnregisteredActivityType", fmt.Sprintf("unregistered activity type %q", resp.ActivityType))
		return
	}

	inputPtr := reflect.New(act.inputType)
	if err := converter.Default.FromPayload(resp.Input, inputPtr.Interface()); err != nil {
		w.respondActivityTaskFailed(ctx, resp.TaskToken, "InputUnmarshalError", err.Error())
		return
	}
	actCtx := activity.NewContext(ctx, activity.Info{
		ActivityID: resp.ActivityId, ActivityType: resp.ActivityType, Attempt: resp.CurrentAttempt,
	})

	var out []reflect.Value
	panicErr := func() (perr error) {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					perr = e
				} else {
					perr = fmt.Errorf("activity panic: %v", r)
				}
			}
		}()
		out = act.fn.Call([]reflect.Value{reflect.ValueOf(actCtx), inputPtr.Elem()})
		return nil
	}()
	if panicErr != nil {
		w.respondActivityTaskFailed(ctx, resp.TaskToken, fmt.Sprintf("%T", panicErr), panicErr.Error())
		return
	}

	if errVal := out[1]; !errVal.IsNil() {
		err := errVal.Interface().(error)
		w.respondActivityTaskFailed(ctx, resp.TaskToken, fmt.Sprintf("%T", err), err.Error())
		return
	}

	payload, err := converter.Default.ToPayload(out[0].Interface())
	if err != nil {
		w.respondActivityTaskFailed(ctx, resp.TaskToken, "MarshalError", err.Error())
		return
	}
	if _, err := w.rpc.RespondActivityTaskCompleted(ctx, &flowv1.RespondActivityTaskCompletedRequest{
		TaskToken: resp.TaskToken, Result: payload, Identity: w.identity,
	}); err != nil {
		w.logger.Error("respond activity task completed", "error", err)
	}
}

func (w *Worker) respondActivityTaskFailed(ctx context.Context, token []byte, errType, message string) {
	if _, err := w.rpc.RespondActivityTaskFailed(ctx, &flowv1.RespondActivityTaskFailedRequest{
		TaskToken: token, Failure: &flowv1.Failure{Type: errType, Message: message}, Identity: w.identity,
	}); err != nil {
		w.logger.Error("respond activity task failed", "error", err)
	}
}
