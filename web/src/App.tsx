import { BrowserRouter, Route, Routes } from "react-router-dom";
import WorkflowDetailPage from "./pages/WorkflowDetailPage";
import WorkflowsListPage from "./pages/WorkflowsListPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="app-shell">
        <header className="app-header">
          <span className="app-title">flowd</span>
        </header>
        <main className="app-main">
          <Routes>
            <Route path="/" element={<WorkflowsListPage />} />
            <Route path="/workflows/:workflowId/:runId" element={<WorkflowDetailPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
