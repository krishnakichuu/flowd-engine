// Mirrors internal/webapi's JSON shapes, which are protojson-encoded
// (UseProtoNames: true, so snake_case throughout — see
// internal/webapi/server.go) proto messages plus a few hand-written
// wrapper keys (next_page_token, namespaces, executions, events).

export interface WorkflowExecution {
  workflow_id: string;
  run_id: string;
}

export type WorkflowExecutionStatus =
  | "WORKFLOW_EXECUTION_STATUS_UNSPECIFIED"
  | "WORKFLOW_EXECUTION_STATUS_RUNNING"
  | "WORKFLOW_EXECUTION_STATUS_COMPLETED"
  | "WORKFLOW_EXECUTION_STATUS_FAILED"
  | "WORKFLOW_EXECUTION_STATUS_TERMINATED"
  | "WORKFLOW_EXECUTION_STATUS_TIMED_OUT"
  | "WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW";

export interface WorkflowExecutionInfo {
  execution: WorkflowExecution;
  workflow_type: string;
  task_queue: string;
  status: WorkflowExecutionStatus;
  start_time: string;
  close_time?: string;
}

export interface ListWorkflowsResponse {
  executions: WorkflowExecutionInfo[];
  next_page_token: string;
}

export interface DescribeWorkflowResponse {
  execution: WorkflowExecution;
  workflow_type: string;
  task_queue: string;
  status: WorkflowExecutionStatus;
}

export interface HistoryEvent {
  event_id: string;
  event_time: string;
  event_type: string;
  // The event's oneof attributes aren't rendered in v1 — same scope as
  // `flow-cli history`, which only prints event_id/event_type/event_time.
  [key: string]: unknown;
}

export interface GetWorkflowHistoryResponse {
  events: HistoryEvent[];
}

export interface NamespaceInfo {
  name: string;
  created_at: string;
}

export interface ListNamespacesResponse {
  namespaces: NamespaceInfo[];
}
