package replayer

import (
	"testing"
)

func TestExecuteRoundRunsToBlockingPoint(t *testing.T) {
	d := NewDispatcher()
	var trace []string

	ready := false
	c := d.Go(func(c *Coroutine) {
		trace = append(trace, "start")
		for !ready {
			c.Yield()
		}
		trace = append(trace, "unblocked")
	})

	d.ExecuteRound()
	if got := trace; len(got) != 1 || got[0] != "start" {
		t.Fatalf("after first round, trace = %v, want [start] (coroutine should block on Yield)", got)
	}
	if c.Done() {
		t.Fatal("coroutine reported done after blocking on Yield")
	}

	ready = true
	d.ExecuteRound()
	if got := trace; len(got) != 2 || got[1] != "unblocked" {
		t.Fatalf("after second round, trace = %v, want [start unblocked]", got)
	}
	if !c.Done() {
		t.Fatal("coroutine should be done after returning")
	}
}

func TestExecuteRoundFixedOrder(t *testing.T) {
	d := NewDispatcher()
	var order []int

	for i := 0; i < 3; i++ {
		i := i
		d.Go(func(c *Coroutine) { order = append(order, i) })
	}
	d.ExecuteRound()

	want := []int{0, 1, 2}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v (dispatch order must be fixed creation order for replay determinism)", order, want)
		}
	}
}

func TestCoroutinePanicIsRecovered(t *testing.T) {
	d := NewDispatcher()
	d.Go(func(c *Coroutine) { panic("boom") })
	d.ExecuteRound()

	if !d.AllDone() {
		t.Fatal("panicking coroutine should still be marked done")
	}
	err := d.FirstPanic()
	if err == nil {
		t.Fatal("expected a recovered panic error, got nil")
	}
	if got := err.Error(); got != "workflow panic: boom" {
		t.Fatalf("panic error = %q, want %q", got, "workflow panic: boom")
	}
}

func TestActivityFutureResolvesFromHistory(t *testing.T) {
	// Simulates ScheduleActivity's replay path directly: an activity
	// already present in activityOutcomes resolves within the same round
	// without the coroutine ever blocking on Yield.
	e := NewExecution()
	e.scheduledActivities = []scheduledActivity{{ActivityID: 1, ActivityType: "Foo"}}
	e.activityOutcomes[1] = ActivityOutcome{Result: nil}

	var resolved bool
	e.Dispatcher.Go(func(c *Coroutine) {
		f := e.ScheduleActivity("Foo", nil, ActivityOptions{})
		f.Get(c)
		resolved = true
	})
	e.Dispatcher.ExecuteRound()

	if !resolved {
		t.Fatal("future should have resolved within one round when its outcome is already in history")
	}
	if !e.Dispatcher.AllDone() {
		t.Fatal("coroutine should have completed, not blocked")
	}
}

func TestScheduleActivityDetectsNonDeterminism(t *testing.T) {
	e := NewExecution()
	e.scheduledActivities = []scheduledActivity{{ActivityID: 1, ActivityType: "Foo"}}
	e.activityOutcomes[1] = ActivityOutcome{}

	e.Dispatcher.Go(func(c *Coroutine) {
		// History recorded "Foo" at this position; scheduling "Bar" instead
		// must be caught as non-determinism, not silently accepted.
		e.ScheduleActivity("Bar", nil, ActivityOptions{})
	})
	e.Dispatcher.ExecuteRound()

	err := e.Dispatcher.FirstPanic()
	if err == nil {
		t.Fatal("expected a non-determinism panic, got nil")
	}
	if _, ok := err.(*NonDeterministicError); !ok {
		t.Fatalf("expected *NonDeterministicError, got %T: %v", err, err)
	}
}
