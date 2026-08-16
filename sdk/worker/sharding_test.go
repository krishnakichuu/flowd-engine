package worker

import (
	"testing"

	"github.com/krishnakichuu/flowd/sdk/client"
)

// TestNewThreadsTaskQueuePartitions proves Options.TaskQueuePartitions
// actually reaches the Worker (Phase 2 roadmap, Track C, item 3) — a
// worker that declared a subset of partitions but silently polled for
// everything anyway would defeat the whole feature.
func TestNewThreadsTaskQueuePartitions(t *testing.T) {
	c, err := client.Dial("localhost:0", client.Options{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	want := []int32{2, 5}
	w := New(c, "some-queue", Options{TaskQueuePartitions: want})

	if len(w.taskQueuePartitions) != len(want) {
		t.Fatalf("taskQueuePartitions = %v, want %v", w.taskQueuePartitions, want)
	}
	for i, p := range want {
		if w.taskQueuePartitions[i] != p {
			t.Fatalf("taskQueuePartitions = %v, want %v", w.taskQueuePartitions, want)
		}
	}
}

// TestNewDefaultsToNilTaskQueuePartitions confirms the zero-value Options
// (every caller before this feature existed) leaves partitions unset —
// visible to every partition, unchanged behavior.
func TestNewDefaultsToNilTaskQueuePartitions(t *testing.T) {
	c, err := client.Dial("localhost:0", client.Options{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	w := New(c, "some-queue", Options{})
	if len(w.taskQueuePartitions) != 0 {
		t.Fatalf("taskQueuePartitions = %v, want empty", w.taskQueuePartitions)
	}
}
