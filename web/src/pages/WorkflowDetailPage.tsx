import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { describeWorkflow, getWorkflowHistory } from "../api/client";
import type { DescribeWorkflowResponse, HistoryEvent } from "../api/types";

function formatTime(iso: string | undefined): string {
  if (!iso) return "–";
  return new Date(iso).toLocaleString();
}

export default function WorkflowDetailPage() {
  const { workflowId = "", runId = "" } = useParams();
  const [summary, setSummary] = useState<DescribeWorkflowResponse | null>(null);
  const [events, setEvents] = useState<HistoryEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    Promise.all([describeWorkflow(workflowId, runId), getWorkflowHistory(workflowId, runId)])
      .then(([desc, hist]) => {
        setSummary(desc);
        setEvents(hist.events ?? []);
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [workflowId, runId]);

  return (
    <div>
      <p>
        <Link to="/">&larr; Back to workflows</Link>
      </p>
      <h1>{workflowId}</h1>
      <p className="mono-cell subtle">{runId}</p>

      {error && <div className="error-banner">Failed to load workflow: {error}</div>}
      {loading && <p>Loading…</p>}

      {summary && (
        <div className="summary-card">
          <div>
            <span className="summary-label">Type</span>
            <span>{summary.workflow_type}</span>
          </div>
          <div>
            <span className="summary-label">Task Queue</span>
            <span>{summary.task_queue}</span>
          </div>
          <div>
            <span className="summary-label">Status</span>
            <span>{summary.status.replace("WORKFLOW_EXECUTION_STATUS_", "")}</span>
          </div>
        </div>
      )}

      <h2>History</h2>
      <ol className="history-timeline">
        {events.map((ev) => (
          <li key={ev.event_id}>
            <span className="history-event-id">#{ev.event_id}</span>
            <span className="history-event-type">{ev.event_type.replace("HISTORY_EVENT_TYPE_", "")}</span>
            <span className="history-event-time">{formatTime(ev.event_time)}</span>
          </li>
        ))}
      </ol>
    </div>
  );
}
