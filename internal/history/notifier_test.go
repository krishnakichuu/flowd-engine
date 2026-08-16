package history

import (
	"context"
	"testing"
	"time"
)

func TestNotifierWaitReturnsImmediatelyOnNotify(t *testing.T) {
	n := newNotifier()
	key := workflowQueueKey(1, "q")

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- n.wait(context.Background(), key)
	}()

	// Give the goroutine a moment to register before notifying, without
	// relying on a fixed sleep matching taskWaitBackoff.
	time.Sleep(10 * time.Millisecond)
	n.notify(key)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after notify")
	}

	if elapsed := time.Since(start); elapsed >= taskWaitBackoff {
		t.Fatalf("wait took %v, expected notify to short-circuit taskWaitBackoff (%v)", elapsed, taskWaitBackoff)
	}
}

func TestNotifierWaitFallsBackToBackoffWithoutNotify(t *testing.T) {
	n := newNotifier()
	start := time.Now()

	if err := n.wait(context.Background(), workflowQueueKey(1, "q")); err != nil {
		t.Fatalf("wait returned error: %v", err)
	}

	if elapsed := time.Since(start); elapsed < taskWaitBackoff {
		t.Fatalf("wait returned after %v, expected at least taskWaitBackoff (%v)", elapsed, taskWaitBackoff)
	}
}

func TestNotifierWaitReturnsOnContextCancel(t *testing.T) {
	n := newNotifier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := n.wait(ctx, workflowQueueKey(1, "q"))
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
}

func TestNotifierNotifyWakesAllWaiters(t *testing.T) {
	n := newNotifier()
	key := activityQueueKey(1, "q")

	const waiters = 5
	done := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			done <- n.wait(context.Background(), key)
		}()
	}
	time.Sleep(10 * time.Millisecond)
	n.notify(key)

	for i := 0; i < waiters; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("wait returned error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("not all waiters woke after notify")
		}
	}
}

func TestNotifierNotifyWithNoWaitersDoesNotPanic(t *testing.T) {
	n := newNotifier()
	n.notify(workflowQueueKey(1, "nobody-listening"))
}

func TestNotifierRegisterCancelRemovesWaiter(t *testing.T) {
	n := newNotifier()
	key := workflowQueueKey(1, "q")

	_, cancel := n.register(key)
	if len(n.waiters[key]) != 1 {
		t.Fatalf("expected 1 waiter after register, got %d", len(n.waiters[key]))
	}
	cancel()
	if _, ok := n.waiters[key]; ok {
		t.Fatalf("expected waiter entry removed after cancel, key still present with %d waiters", len(n.waiters[key]))
	}
}
