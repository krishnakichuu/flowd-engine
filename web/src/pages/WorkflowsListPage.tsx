import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { listWorkflows } from "../api/client";
import type { WorkflowExecutionInfo, WorkflowExecutionStatus } from "../api/types";

const STATUS_OPTIONS: { label: string; value: WorkflowExecutionStatus | "" }[] = [
  { label: "All statuses", value: "" },
  { label: "Running", value: "WORKFLOW_EXECUTION_STATUS_RUNNING" },
  { label: "Completed", value: "WORKFLOW_EXECUTION_STATUS_COMPLETED" },
  { label: "Failed", value: "WORKFLOW_EXECUTION_STATUS_FAILED" },
  { label: "Terminated", value: "WORKFLOW_EXECUTION_STATUS_TERMINATED" },
  { label: "Timed out", value: "WORKFLOW_EXECUTION_STATUS_TIMED_OUT" },
  { label: "Continued as new", value: "WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW" },
];

function statusLabel(status: WorkflowExecutionStatus): string {
  return status.replace("WORKFLOW_EXECUTION_STATUS_", "");
}

function statusClass(status: WorkflowExecutionStatus): string {
  switch (status) {
    case "WORKFLOW_EXECUTION_STATUS_RUNNING":
      return "badge badge-running";
    case "WORKFLOW_EXECUTION_STATUS_COMPLETED":
      return "badge badge-completed";
    case "WORKFLOW_EXECUTION_STATUS_FAILED":
    case "WORKFLOW_EXECUTION_STATUS_TIMED_OUT":
      return "badge badge-failed";
    case "WORKFLOW_EXECUTION_STATUS_TERMINATED":
      return "badge badge-terminated";
    default:
      return "badge";
  }
}

function formatTime(iso: string | undefined): string {
  if (!iso) return "–";
  return new Date(iso).toLocaleString();
}

export default function WorkflowsListPage() {
  const [status, setStatus] = useState<WorkflowExecutionStatus | "">("");
  const [taskQueue, setTaskQueue] = useState("");
  const [executions, setExecutions] = useState<WorkflowExecutionInfo[]>([]);
  const [pageToken, setPageToken] = useState<string | undefined>(undefined);
  const [pageStack, setPageStack] = useState<string[]>([]);
  const [nextPageToken, setNextPageToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    listWorkflows({ status, taskQueue: taskQueue || undefined, pageToken })
      .then((resp) => {
        setExecutions(resp.executions ?? []);
        setNextPageToken(resp.next_page_token ?? "");
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [status, taskQueue, pageToken]);

  function goNext() {
    if (!nextPageToken) return;
    setPageStack((s) => [...s, pageToken ?? ""]);
    setPageToken(nextPageToken);
  }

  function goPrev() {
    setPageStack((s) => {
      if (s.length === 0) return s;
      const copy = [...s];
      const prev = copy.pop();
      setPageToken(prev || undefined);
      return copy;
    });
  }

  return (
    <div>
      <h1>Workflow Executions</h1>

      <div className="filter-bar">
        <select
          value={status}
          onChange={(e) => {
            setPageStack([]);
            setPageToken(undefined);
            setStatus(e.target.value as WorkflowExecutionStatus | "");
          }}
        >
          {STATUS_OPTIONS.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
        <input
          type="text"
          placeholder="Filter by task queue"
          value={taskQueue}
          onChange={(e) => {
            setPageStack([]);
            setPageToken(undefined);
            setTaskQueue(e.target.value);
          }}
        />
      </div>

      {error && <div className="error-banner">Failed to load workflows: {error}</div>}

      <table className="workflow-table">
        <thead>
          <tr>
            <th>Workflow ID</th>
            <th>Run ID</th>
            <th>Type</th>
            <th>Task Queue</th>
            <th>Status</th>
            <th>Started</th>
            <th>Closed</th>
          </tr>
        </thead>
        <tbody>
          {!loading && executions.length === 0 && (
            <tr>
              <td colSpan={7} className="empty-row">
                No workflow executions found.
              </td>
            </tr>
          )}
          {executions.map((e) => (
            <tr key={`${e.execution.workflow_id}/${e.execution.run_id}`}>
              <td>
                <Link to={`/workflows/${encodeURIComponent(e.execution.workflow_id)}/${encodeURIComponent(e.execution.run_id)}`}>
                  {e.execution.workflow_id}
                </Link>
              </td>
              <td className="mono-cell">{e.execution.run_id}</td>
              <td>{e.workflow_type}</td>
              <td>{e.task_queue}</td>
              <td>
                <span className={statusClass(e.status)}>{statusLabel(e.status)}</span>
              </td>
              <td>{formatTime(e.start_time)}</td>
              <td>{formatTime(e.close_time)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <div className="pager">
        <button onClick={goPrev} disabled={pageStack.length === 0 || loading}>
          &larr; Previous
        </button>
        <button onClick={goNext} disabled={!nextPageToken || loading}>
          Next &rarr;
        </button>
      </div>
    </div>
  );
}
