// Package replayer implements the deterministic coroutine dispatcher that
// underlies workflow.Context: workflow code runs on cooperatively-scheduled
// coroutines (never raw goroutines) so that history replay can reconstruct
// identical execution order, per ADR-0001.
//
// Coroutines are backed by real goroutines, but a Dispatcher only ever lets
// one coroutine execute application code at a time: Coroutine.Yield hands
// control back to the dispatcher and blocks until explicitly resumed. A
// single ExecuteRound resumes every live coroutine exactly once, in a fixed
// (creation) order, and each coroutine runs until its next Yield or
// completion — this fixed order is what makes replay deterministic.
package replayer

import "fmt"

// Coroutine is one cooperatively-scheduled unit of workflow code.
type Coroutine struct {
	resumeCh chan struct{}
	yieldCh  chan struct{}
	done     bool
	panicErr error
}

// Yield hands control back to the dispatcher and blocks until this
// coroutine is resumed on a later round. Every blocking primitive exposed
// by workflow.Context (Future.Get, Sleep) is implemented in terms of this —
// it is the only way workflow code may block, which is what makes replay
// possible.
//
// This same property — a real goroutine parked here rather than unwound —
// is what makes sdk/worker's sticky Execution cache possible at all: an
// Execution kept alive between tasks has coroutines already sitting in
// Yield, ready to resume from exactly where they left off with no replay
// needed. The flip side: if the cache evicts an Execution whose workflow
// hasn't reached a terminal outcome, its coroutines are never resumed
// again and stay parked here permanently — a small, bounded (by cache
// capacity), and deliberately accepted leak rather than adding cooperative
// cancellation to this type, which would put the determinism guarantee
// above at risk for a memory optimization.
func (c *Coroutine) Yield() {
	c.yieldCh <- struct{}{}
	<-c.resumeCh
}

func (c *Coroutine) Done() bool { return c.done }

type Dispatcher struct {
	coroutines []*Coroutine
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{}
}

// Go starts a new coroutine running fn. The coroutine does not begin
// executing until the dispatcher's first ExecuteRound call.
func (d *Dispatcher) Go(fn func(c *Coroutine)) *Coroutine {
	c := &Coroutine{
		resumeCh: make(chan struct{}),
		yieldCh:  make(chan struct{}),
	}
	d.coroutines = append(d.coroutines, c)

	go func() {
		<-c.resumeCh
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok {
					c.panicErr = err
				} else {
					c.panicErr = fmt.Errorf("workflow panic: %v", r)
				}
			}
			c.done = true
			c.yieldCh <- struct{}{}
		}()
		fn(c)
	}()

	return c
}

// ExecuteRound resumes every not-yet-done coroutine exactly once, in
// creation order, waiting for each to yield or finish before resuming the
// next. Any coroutine already blocked in Yield() that has nothing new to
// observe will simply call Yield() again immediately, which is a harmless
// no-op round-trip — callers pace how many rounds they run (see
// Execution.Run), so this never spins.
func (d *Dispatcher) ExecuteRound() {
	for _, c := range d.coroutines {
		if c.done {
			continue
		}
		c.resumeCh <- struct{}{}
		<-c.yieldCh
	}
}

// AllDone reports whether every coroutine has finished (returned or
// panicked).
func (d *Dispatcher) AllDone() bool {
	for _, c := range d.coroutines {
		if !c.done {
			return false
		}
	}
	return true
}

// FirstPanic returns the first coroutine panic recorded, if any, treated as
// a workflow execution failure (not a non-determinism error, which is
// signaled separately — see NonDeterministicError).
func (d *Dispatcher) FirstPanic() error {
	for _, c := range d.coroutines {
		if c.panicErr != nil {
			return c.panicErr
		}
	}
	return nil
}
