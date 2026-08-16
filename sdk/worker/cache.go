package worker

import (
	"container/list"

	"github.com/krishnakichuu/flowd/sdk/internal/replayer"
)

// execCacheKey identifies one run's cache entry. workflow_id alone isn't
// enough — a run that continued as new produces a different run_id under
// the same workflow_id, and the old run's cache entry (if it's even still
// there) must not be confused with the new one's.
type execCacheKey struct {
	WorkflowID string
	RunID      string
}

// cachedExecution is what sticky caching keeps alive between a run's
// workflow tasks: the Execution itself (whose Dispatcher holds real,
// already-running goroutines blocked in Coroutine.Yield — see
// dispatcher.go) plus lastEventID, the cache's bookmark for how much of
// the run's history it has already fed in. The server always sends full
// history on every dispatch (see internal/history's maxHistoryFetch doc);
// lastEventID is what lets the worker slice off just the new tail itself.
type cachedExecution struct {
	exec         *replayer.Execution
	workflowType string
	lastEventID  int64
}

type cacheEntry struct {
	key   execCacheKey
	value *cachedExecution
}

// executionCache is a bounded LRU cache of in-flight workflow Executions,
// keyed by run — the mechanism behind sticky worker caching (Phase 2
// roadmap, Track C, item 1). It is only ever touched from the single
// goroutine that processes workflow tasks (see Worker.pollWorkflowTasks,
// which calls processWorkflowTask synchronously, one task at a time), so
// it needs no locking.
//
// Eviction past capacity just drops the reference — see Coroutine.Yield's
// doc for why that's an accepted, bounded tradeoff rather than something
// this cache tries to clean up after.
type executionCache struct {
	capacity int
	ll       *list.List
	items    map[execCacheKey]*list.Element
}

func newExecutionCache(capacity int) *executionCache {
	return &executionCache{capacity: capacity, ll: list.New(), items: make(map[execCacheKey]*list.Element)}
}

func (c *executionCache) get(key execCacheKey) (*cachedExecution, bool) {
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).value, true
}

// put inserts or refreshes key's entry, evicting the least-recently-used
// entry if this insert pushes the cache past capacity.
func (c *executionCache) put(key execCacheKey, value *cachedExecution) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*cacheEntry).value = value
		return
	}
	el := c.ll.PushFront(&cacheEntry{key: key, value: value})
	c.items[key] = el
	if c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*cacheEntry).key)
	}
}

func (c *executionCache) delete(key execCacheKey) {
	if el, ok := c.items[key]; ok {
		c.ll.Remove(el)
		delete(c.items, key)
	}
}

func (c *executionCache) len() int { return c.ll.Len() }
