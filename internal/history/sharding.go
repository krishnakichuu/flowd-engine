package history

import "hash/fnv"

// PartitionFor deterministically maps key (a workflow_id) into
// [0, numPartitions) — the routing layer ADR-0002 predicted shard_id/
// task_queue_partition would need, added now as constant/zero columns
// specifically so this is "a routing change, not a migration touching
// every row" (Phase 2 roadmap, Track C, item 3). FNV-1a is stable across
// processes and restarts, which is what lets two independent callers — an
// enqueuer computing a task's partition, and a worker later declaring
// which partitions it serves — agree on the same number without
// coordinating with each other.
//
// numPartitions <= 1 always returns 0: a single-partition deployment (the
// default, and every deployment before this feature existed) behaves
// exactly as before.
func PartitionFor(key string, numPartitions int32) int32 {
	if numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key)) // fnv32a.Write never returns an error
	// #nosec G115 -- result is a value mod numPartitions (an int32 > 1
	// here), so it always fits in [0, numPartitions) well within int32
	// range; the uint32 intermediate is just fnv32a's own output type.
	return int32(h.Sum32() % uint32(numPartitions))
}
