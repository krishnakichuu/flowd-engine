package history

import "errors"

var (
	// ErrConcurrentModification is returned when the next_event_id CAS
	// (ADR-0002, Mechanism B) rejects a write because another writer already
	// advanced the version. Callers must discard any in-memory replay state.
	ErrConcurrentModification = errors.New("history: concurrent modification, expected next_event_id no longer current")

	// ErrTaskLeaseExpired is returned when a task-completion write loses the
	// lease-token check (ADR-0002, Mechanism A) — the lease already expired
	// and was reassigned to another worker.
	ErrTaskLeaseExpired = errors.New("history: task lease expired or already completed")

	// ErrNoTaskAvailable is returned by a single dequeue attempt when no
	// pending task exists; callers polling should retry until their deadline.
	ErrNoTaskAvailable = errors.New("history: no task available")

	// ErrWorkflowAlreadyRunning is returned by StartWorkflowExecution when a
	// different run is already open for the same workflow_id.
	ErrWorkflowAlreadyRunning = errors.New("history: workflow already running")

	// ErrWorkflowNotFound is returned when a referenced workflow execution
	// does not exist.
	ErrWorkflowNotFound = errors.New("history: workflow execution not found")

	// ErrNamespaceNotFound is returned when the requested namespace has not
	// been created.
	ErrNamespaceNotFound = errors.New("history: namespace not found")

	// ErrNamespaceAlreadyExists is returned by CreateNamespace when a
	// namespace with that name already exists.
	ErrNamespaceAlreadyExists = errors.New("history: namespace already exists")
)
