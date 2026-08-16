package worker

import "testing"

func TestExecutionCacheGetMiss(t *testing.T) {
	c := newExecutionCache(2)
	if _, ok := c.get(execCacheKey{WorkflowID: "wf", RunID: "r1"}); ok {
		t.Fatal("expected a miss on an empty cache")
	}
}

func TestExecutionCachePutGet(t *testing.T) {
	c := newExecutionCache(2)
	key := execCacheKey{WorkflowID: "wf", RunID: "r1"}
	want := &cachedExecution{workflowType: "T", lastEventID: 5}

	c.put(key, want)
	got, ok := c.get(key)
	if !ok {
		t.Fatal("expected a hit after put")
	}
	if got != want {
		t.Fatalf("get returned %+v, want the exact value put in: %+v", got, want)
	}
	if c.len() != 1 {
		t.Fatalf("len = %d, want 1", c.len())
	}
}

func TestExecutionCacheDelete(t *testing.T) {
	c := newExecutionCache(2)
	key := execCacheKey{WorkflowID: "wf", RunID: "r1"}
	c.put(key, &cachedExecution{})
	c.delete(key)
	if _, ok := c.get(key); ok {
		t.Fatal("expected a miss after delete")
	}
	if c.len() != 0 {
		t.Fatalf("len = %d, want 0", c.len())
	}
}

func TestExecutionCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := newExecutionCache(2)
	k1 := execCacheKey{WorkflowID: "wf", RunID: "r1"}
	k2 := execCacheKey{WorkflowID: "wf", RunID: "r2"}
	k3 := execCacheKey{WorkflowID: "wf", RunID: "r3"}

	c.put(k1, &cachedExecution{lastEventID: 1})
	c.put(k2, &cachedExecution{lastEventID: 2})
	// Touch k1 so it's more recently used than k2.
	if _, ok := c.get(k1); !ok {
		t.Fatal("expected a hit on k1")
	}
	// Capacity is 2; inserting a third entry must evict the least recently
	// used one — that's k2, not k1 (k1 was just touched above).
	c.put(k3, &cachedExecution{lastEventID: 3})

	if _, ok := c.get(k2); ok {
		t.Fatal("k2 should have been evicted as least-recently-used")
	}
	if _, ok := c.get(k1); !ok {
		t.Fatal("k1 should still be cached — it was touched before the eviction")
	}
	if _, ok := c.get(k3); !ok {
		t.Fatal("k3 should be cached — it was just inserted")
	}
	if c.len() != 2 {
		t.Fatalf("len = %d, want 2 (capacity)", c.len())
	}
}

func TestExecutionCachePutExistingKeyRefreshesValueAndRecency(t *testing.T) {
	c := newExecutionCache(1)
	key := execCacheKey{WorkflowID: "wf", RunID: "r1"}
	c.put(key, &cachedExecution{lastEventID: 1})
	c.put(key, &cachedExecution{lastEventID: 2})

	got, ok := c.get(key)
	if !ok {
		t.Fatal("expected a hit")
	}
	if got.lastEventID != 2 {
		t.Fatalf("lastEventID = %d, want 2 (the second put should replace the first)", got.lastEventID)
	}
	if c.len() != 1 {
		t.Fatalf("len = %d, want 1 — re-putting the same key must not grow the cache", c.len())
	}
}
