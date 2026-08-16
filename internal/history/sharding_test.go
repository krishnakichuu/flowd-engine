package history

import "testing"

func TestPartitionForSingleOrZeroPartitionsAlwaysZero(t *testing.T) {
	for _, n := range []int32{0, 1, -1} {
		if got := PartitionFor("any-workflow-id", n); got != 0 {
			t.Fatalf("PartitionFor(_, %d) = %d, want 0", n, got)
		}
	}
}

func TestPartitionForIsDeterministic(t *testing.T) {
	const key = "order-48291"
	first := PartitionFor(key, 8)
	for i := 0; i < 100; i++ {
		if got := PartitionFor(key, 8); got != first {
			t.Fatalf("PartitionFor(%q, 8) = %d on call %d, want %d (must be stable across calls)", key, got, i, first)
		}
	}
}

func TestPartitionForStaysInRange(t *testing.T) {
	numPartitions := int32(16)
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + i%26))
		got := PartitionFor(key, numPartitions)
		if got < 0 || got >= numPartitions {
			t.Fatalf("PartitionFor(%q, %d) = %d, out of range [0, %d)", key, numPartitions, got, numPartitions)
		}
	}
}

// TestPartitionForDistributesAcrossPartitions is a coarse sanity check,
// not a statistical proof: with a reasonable number of distinct keys, a
// hash-based partitioner shouldn't collapse everything onto one partition.
func TestPartitionForDistributesAcrossPartitions(t *testing.T) {
	const numPartitions = 8
	seen := make(map[int32]int)
	for i := 0; i < 400; i++ {
		key := "workflow-" + string(rune('A'+i%26)) + string(rune('0'+i%10))
		seen[PartitionFor(key, numPartitions)]++
	}
	if len(seen) < numPartitions/2 {
		t.Fatalf("only %d of %d partitions were used across 400 distinct-ish keys — suspiciously clustered: %v", len(seen), numPartitions, seen)
	}
}
