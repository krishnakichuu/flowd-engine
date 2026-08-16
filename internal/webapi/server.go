// Package webapi is the JSON/HTTP backend-for-frontend the web UI
// (internal/webui) talks to instead of raw gRPC (Phase 2 roadmap, Track D,
// item 3). It wraps sdk/client.Client — the same client any Go SDK
// consumer or flow-cli uses — rather than reaching into internal/history
// directly, so this server has no special-cased internal path into
// flowd's core and would keep working unmodified if it were ever split
// out to point at a remote flowd instance. Deliberately read-only (every
// route is GET): v1 scope is inspection, not triggering mutating actions
// from a browser.
package webapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// UseProtoNames keeps every JSON key snake_case, matching this package's
// own hand-written wrapper keys (next_page_token, task_queue, ...) —
// without it, protojson's default is lowerCamelCase, which would mix
// naming conventions within the same response body.
var marshaler = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true}

type server struct {
	client *client.Client
	logger *slog.Logger
}

// NewServer returns the /api/* handler. c is expected to already be
// dialed against the flowd server this process is embedded in (see
// cmd/flowd/main.go) — this package does not manage the client's
// lifecycle.
func NewServer(c *client.Client, logger *slog.Logger) http.Handler {
	s := &server{client: c, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/namespaces", s.handleListNamespaces)
	mux.HandleFunc("GET /api/workflows", s.handleListWorkflows)
	mux.HandleFunc("GET /api/workflows/{workflowId}/{runId}", s.handleDescribeWorkflow)
	mux.HandleFunc("GET /api/workflows/{workflowId}/{runId}/history", s.handleGetWorkflowHistory)
	return mux
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, map[string]string{"status": "ok"})
}

func (s *server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	namespaces, err := s.client.ListNamespaces(r.Context())
	if err != nil {
		s.writeError(w, err)
		return
	}
	raw := make([]json.RawMessage, len(namespaces))
	for i, ns := range namespaces {
		b, err := protoJSON(ns)
		if err != nil {
			s.writeError(w, err)
			return
		}
		raw[i] = b
	}
	s.writeJSON(w, map[string]any{"namespaces": raw})
}

func (s *server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	statusFilter, err := parseStatusFilter(q.Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var pageSize int32
	if v := q.Get("page_size"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "invalid page_size: "+err.Error(), http.StatusBadRequest)
			return
		}
		pageSize = int32(n)
	}

	result, err := s.client.ListWorkflows(r.Context(), client.ListWorkflowsOptions{
		StatusFilter: statusFilter, TaskQueue: q.Get("task_queue"),
		PageSize: pageSize, PageToken: []byte(q.Get("page_token")),
	})
	if err != nil {
		s.writeError(w, err)
		return
	}
	executions := make([]json.RawMessage, len(result.Executions))
	for i, e := range result.Executions {
		b, err := protoJSON(e)
		if err != nil {
			s.writeError(w, err)
			return
		}
		executions[i] = b
	}
	s.writeJSON(w, map[string]any{
		"executions":      executions,
		"next_page_token": string(result.NextPageToken),
	})
}

func (s *server) handleDescribeWorkflow(w http.ResponseWriter, r *http.Request) {
	resp, err := s.client.DescribeWorkflow(r.Context(), r.PathValue("workflowId"), r.PathValue("runId"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeProto(w, resp)
}

func (s *server) handleGetWorkflowHistory(w http.ResponseWriter, r *http.Request) {
	events, err := s.client.GetWorkflowHistory(r.Context(), r.PathValue("workflowId"), r.PathValue("runId"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	raw := make([]json.RawMessage, len(events))
	for i, ev := range events {
		b, err := protoJSON(ev)
		if err != nil {
			s.writeError(w, err)
			return
		}
		raw[i] = b
	}
	s.writeJSON(w, map[string]any{"events": raw})
}

// parseStatusFilter mirrors cmd/flow-cli's identically-named helper — kept
// as a small local copy rather than a shared dependency since both are
// ~15 lines translating the same friendly strings to the proto enum, and
// flow-cli's copy lives in package main (unimportable).
func parseStatusFilter(s string) (flowv1.WorkflowExecutionStatus, error) {
	switch strings.ToLower(s) {
	case "":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, nil
	case "running":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_RUNNING, nil
	case "completed":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED, nil
	case "failed":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_FAILED, nil
	case "terminated":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	case "timed_out":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil
	case "continued_as_new":
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, nil
	default:
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, &statusFilterError{s}
	}
}

type statusFilterError struct{ value string }

func (e *statusFilterError) Error() string { return "unknown status filter " + strconv.Quote(e.value) }

func protoJSON(msg proto.Message) (json.RawMessage, error) {
	b, err := marshaler.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func (s *server) writeProto(w http.ResponseWriter, msg proto.Message) {
	b, err := protoJSON(msg)
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (s *server) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("webapi: encode response", "error", err)
	}
}

// writeError maps the gRPC status code sdk/client.Client surfaces (the
// server-side mapping is internal/frontend's toGRPCError) to a matching
// HTTP status, so a browser gets ordinary REST semantics without this
// package needing to import internal/history's sentinel errors directly.
func (s *server) writeError(w http.ResponseWriter, err error) {
	httpStatus := http.StatusInternalServerError
	switch status.Code(err) {
	case codes.NotFound:
		httpStatus = http.StatusNotFound
	case codes.InvalidArgument:
		httpStatus = http.StatusBadRequest
	case codes.AlreadyExists:
		httpStatus = http.StatusConflict
	case codes.Aborted, codes.DeadlineExceeded:
		httpStatus = http.StatusConflict
	case codes.Unauthenticated:
		httpStatus = http.StatusUnauthorized
	case codes.PermissionDenied:
		httpStatus = http.StatusForbidden
	}
	s.logger.Error("webapi: request failed", "error", err, "http_status", httpStatus)
	http.Error(w, err.Error(), httpStatus)
}
