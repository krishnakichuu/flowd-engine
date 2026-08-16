package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/activity"
	"google.golang.org/grpc"
)

// fakePollClient implements flowv1.WorkflowServiceClient by embedding it
// (satisfies the interface; only the methods overridden below are ever
// called on this path) and hands out a fixed number of activity tasks
// before blocking on ctx, the same shape a real long-poll takes once the
// queue is drained.
type fakePollClient struct {
	flowv1.WorkflowServiceClient

	mu        sync.Mutex
	tasksLeft int
}

func (f *fakePollClient) PollActivityTaskQueue(ctx context.Context, _ *flowv1.PollActivityTaskQueueRequest, _ ...grpc.CallOption) (*flowv1.PollActivityTaskQueueResponse, error) {
	f.mu.Lock()
	if f.tasksLeft > 0 {
		f.tasksLeft--
		f.mu.Unlock()
		return &flowv1.PollActivityTaskQueueResponse{
			TaskToken:    []byte("tok"),
			ActivityType: "SlowActivity",
			Input:        &flowv1.Payload{Data: []byte(`""`)},
		}, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return &flowv1.PollActivityTaskQueueResponse{}, ctx.Err()
}

func (f *fakePollClient) RespondActivityTaskCompleted(context.Context, *flowv1.RespondActivityTaskCompletedRequest, ...grpc.CallOption) (*flowv1.RespondActivityTaskCompletedResponse, error) {
	return &flowv1.RespondActivityTaskCompletedResponse{}, nil
}

func (f *fakePollClient) RespondActivityTaskFailed(context.Context, *flowv1.RespondActivityTaskFailedRequest, ...grpc.CallOption) (*flowv1.RespondActivityTaskFailedResponse, error) {
	return &flowv1.RespondActivityTaskFailedResponse{}, nil
}

var (
	slowActivityCurrent     int32
	slowActivityMaxObserved int32
)

// SlowActivity is a package-level function, not a closure, so
// RegisterActivity's funcname-derived registration name ("SlowActivity")
// matches the ActivityType fakePollClient hands out above.
func SlowActivity(_ activity.Context, _ string) (string, error) {
	cur := atomic.AddInt32(&slowActivityCurrent, 1)
	for {
		old := atomic.LoadInt32(&slowActivityMaxObserved)
		if cur <= old {
			break
		}
		if atomic.CompareAndSwapInt32(&slowActivityMaxObserved, old, cur) {
			break
		}
	}
	time.Sleep(15 * time.Millisecond)
	atomic.AddInt32(&slowActivityCurrent, -1)
	return "ok", nil
}

// TestPollActivityTasksBoundsConcurrency proves the fix directly: a burst
// of ready activity tasks must never drive more than MaxConcurrentActivities
// concurrent processActivityTask executions, where previously it was
// unbounded (one goroutine per dequeued task, no cap).
func TestPollActivityTasksBoundsConcurrency(t *testing.T) {
	atomic.StoreInt32(&slowActivityCurrent, 0)
	atomic.StoreInt32(&slowActivityMaxObserved, 0)

	const maxConcurrent = 3
	const totalTasks = 20

	fake := &fakePollClient{tasksLeft: totalTasks}
	w := &Worker{
		rpc:         fake,
		activities:  make(map[string]activityFunc),
		activitySem: make(chan struct{}, maxConcurrent),
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	w.RegisterActivity(SlowActivity)

	// 20 tasks at ~15ms each with 3-way concurrency finish in ~100ms;
	// 400ms leaves comfortable headroom without making the test slow.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	w.pollActivityTasks(ctx)

	// pollActivityTasks returns once ctx is done, but in-flight goroutines
	// from the last dispatched batch may still be finishing up.
	deadline := time.Now().Add(1 * time.Second)
	for atomic.LoadInt32(&slowActivityCurrent) > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := atomic.LoadInt32(&slowActivityMaxObserved); got > maxConcurrent {
		t.Fatalf("observed %d concurrent activity executions, want <= %d (the cap)", got, maxConcurrent)
	}
	if got := atomic.LoadInt32(&slowActivityMaxObserved); got == 0 {
		t.Fatal("no activity executions were observed at all — test setup is broken")
	}
}
