import type {
  DescribeWorkflowResponse,
  GetWorkflowHistoryResponse,
  ListNamespacesResponse,
  ListWorkflowsResponse,
  WorkflowExecutionStatus,
} from "./types";

// Same-origin: the built SPA is served by the same Go process as /api/*
// (internal/webui embeds it into cmd/flowd), so no base URL or CORS setup
// is needed — see the dev-time proxy in vite.config.ts for local dev
// against a separately-running flowd.
async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body || path}`);
  }
  return res.json() as Promise<T>;
}

export interface ListWorkflowsParams {
  status?: WorkflowExecutionStatus | "";
  taskQueue?: string;
  pageSize?: number;
  pageToken?: string;
}

export function listWorkflows(params: ListWorkflowsParams = {}): Promise<ListWorkflowsResponse> {
  const q = new URLSearchParams();
  if (params.status) q.set("status", statusToQueryValue(params.status));
  if (params.taskQueue) q.set("task_queue", params.taskQueue);
  if (params.pageSize) q.set("page_size", String(params.pageSize));
  if (params.pageToken) q.set("page_token", params.pageToken);
  const qs = q.toString();
  return getJSON(`/api/workflows${qs ? `?${qs}` : ""}`);
}

export function describeWorkflow(workflowId: string, runId: string): Promise<DescribeWorkflowResponse> {
  return getJSON(`/api/workflows/${encodeURIComponent(workflowId)}/${encodeURIComponent(runId)}`);
}

export function getWorkflowHistory(workflowId: string, runId: string): Promise<GetWorkflowHistoryResponse> {
  return getJSON(`/api/workflows/${encodeURIComponent(workflowId)}/${encodeURIComponent(runId)}/history`);
}

export function listNamespaces(): Promise<ListNamespacesResponse> {
  return getJSON(`/api/namespaces`);
}

// statusToQueryValue maps the enum's full wire name down to the short
// lowercase form internal/webapi.parseStatusFilter (and flow-cli's
// identically-shaped helper) accept.
function statusToQueryValue(status: WorkflowExecutionStatus): string {
  return status.replace("WORKFLOW_EXECUTION_STATUS_", "").toLowerCase();
}
