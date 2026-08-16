// Package metrics defines flowd's Prometheus metrics. Every component must
// expose enough observability to debug it in production without attaching a
// debugger — these counters are the server-side half of that (the SDK
// exposes its own, e.g. workflow_nondeterministic_errors_total).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	WorkflowStartedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flowd_workflow_started_total",
		Help: "Total number of workflow executions started (new runs, not idempotent replays).",
	})

	WorkflowCompletedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flowd_workflow_completed_total",
		Help: "Total number of workflow executions closed, by terminal status.",
	}, []string{"status"})

	ActivityRetryTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flowd_activity_retry_total",
		Help: "Total number of activity task retries scheduled after a failed attempt.",
	})

	WorkflowTaskFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flowd_workflow_task_failed_total",
		Help: "Total number of workflow tasks a worker reported it could not process (panic or detected non-determinism). The task is redispatched, not retried with backoff, so a workflow stuck repeatedly failing shows up here as a fast-climbing counter rather than a slow one.",
	})

	WorkflowTaskDispatchedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flowd_workflow_task_dispatched_total",
		Help: "Total number of workflow tasks dispatched to a worker.",
	})

	ActivityTaskDispatchedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flowd_activity_task_dispatched_total",
		Help: "Total number of activity tasks dispatched to a worker.",
	})
)
