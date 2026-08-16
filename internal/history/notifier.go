package history

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// taskWaitBackoff bounds how long WaitForWorkflowTask/WaitForActivityTask
// block before returning anyway when no in-process notify arrives — the
// safety net for enqueues this process wasn't told about (a task queue
// shared with another flowd instance against the same database, or an
// activity retry becoming due). It was the matching package's fixed
// long-poll retry interval before sync match existed (Phase 2 roadmap,
// Track C, item 2); now it is only the ceiling on that fallback path, not
// the common-case latency.
const taskWaitBackoff = 200 * time.Millisecond

// notifier lets an in-memory task enqueue wake any goroutine currently
// long-polling the same task queue in this process, collapsing dispatch
// latency from "up to taskWaitBackoff" to "immediate" for the common
// single-instance case. It is purely a latency/DB-load optimization on
// top of Postgres FOR UPDATE SKIP LOCKED (ADR-0002, Mechanism A), which
// remains the actual correctness mechanism: a signal is only ever a hint
// to retry the real dequeue, never a guarantee a task is there (another
// waiter, or another process entirely, may have already taken it) — so a
// missed, coalesced, or spurious signal can never cause an incorrect
// dispatch, only a wasted retry.
type notifier struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func newNotifier() *notifier {
	return &notifier{waiters: make(map[string][]chan struct{})}
}

// register adds a waiter for key and returns a channel that receives a
// value when notify(key) next fires, plus a cancel func the caller must
// invoke (directly or via defer) once it stops waiting, so an abandoned
// wait (ctx canceled, backoff elapsed) doesn't leak the entry forever.
func (n *notifier) register(key string) (ch chan struct{}, cancel func()) {
	ch = make(chan struct{}, 1)
	n.mu.Lock()
	n.waiters[key] = append(n.waiters[key], ch)
	n.mu.Unlock()

	cancel = func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		ws := n.waiters[key]
		for i, w := range ws {
			if w == ch {
				n.waiters[key] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(n.waiters[key]) == 0 {
			delete(n.waiters, key)
		}
	}
	return ch, cancel
}

// notify wakes every current waiter on key. It does not queue the signal
// for waiters that register afterward — a task enqueued with nobody
// listening yet is exactly what taskWaitBackoff exists to still catch.
func (n *notifier) notify(key string) {
	n.mu.Lock()
	ws := n.waiters[key]
	delete(n.waiters, key)
	n.mu.Unlock()
	for _, ch := range ws {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// wait blocks until key is notified, taskWaitBackoff elapses, or ctx is
// done — whichever comes first. It returns nil in the first two cases
// (both mean "try the real dequeue again") and ctx.Err() in the third.
func (n *notifier) wait(ctx context.Context, key string) error {
	ch, cancel := n.register(key)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	case <-time.After(taskWaitBackoff):
		return nil
	}
}

func workflowQueueKey(nsID int64, taskQueue string) string {
	return fmt.Sprintf("wf/%d/%s", nsID, taskQueue)
}

func activityQueueKey(nsID int64, taskQueue string) string {
	return fmt.Sprintf("act/%d/%s", nsID, taskQueue)
}

// WaitForWorkflowTask blocks until a workflow task may be available for
// (namespace, taskQueue) — see notifier.wait — so a caller like
// matching.PollWorkflowTask can retry DispatchWorkflowTask immediately on
// enqueue instead of always sleeping out the full backoff.
func (s *Store) WaitForWorkflowTask(ctx context.Context, namespace, taskQueue string) error {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return err
	}
	return s.notifier.wait(ctx, workflowQueueKey(nsID, taskQueue))
}

// WaitForActivityTask is WaitForWorkflowTask's activity-queue counterpart.
func (s *Store) WaitForActivityTask(ctx context.Context, namespace, taskQueue string) error {
	nsID, err := s.getNamespaceID(ctx, namespace)
	if err != nil {
		return err
	}
	return s.notifier.wait(ctx, activityQueueKey(nsID, taskQueue))
}
