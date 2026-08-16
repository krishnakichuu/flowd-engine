// Package frontend implements flowv1.WorkflowServiceServer, the single
// external gRPC contract both clients and worker poll/respond loops talk
// to (see api/proto/flow/api/v1/service.proto, ADR-0001). It validates
// requests and delegates to internal/history and internal/matching; it
// holds no state of its own.
package frontend

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/history"
	"github.com/krishnakichuu/flowd/internal/matching"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// defaultLongPollTimeout bounds PollWorkflowTaskQueue/PollActivityTaskQueue
// (default from the plan: ~60s).
const defaultLongPollTimeout = 60 * time.Second

const defaultNamespace = "default"

type Server struct {
	flowv1.UnimplementedWorkflowServiceServer

	store  *history.Store
	logger *slog.Logger
}

func NewServer(store *history.Store, logger *slog.Logger) *Server {
	return &Server{store: store, logger: logger}
}

func namespaceOrDefault(ns string) string {
	if ns == "" {
		return defaultNamespace
	}
	return ns
}

func (s *Server) StartWorkflowExecution(ctx context.Context, req *flowv1.StartWorkflowExecutionRequest) (*flowv1.StartWorkflowExecutionResponse, error) {
	if err := checkPayloadSize(req.Input); err != nil {
		return nil, err
	}
	requestID := req.RequestId
	if requestID == "" {
		requestID = uuid.NewString()
	}
	runID, err := s.store.StartWorkflowExecution(ctx, history.StartWorkflowExecutionParams{
		Namespace:                namespaceOrDefault(req.Namespace),
		WorkflowID:               req.WorkflowId,
		WorkflowType:             req.WorkflowType,
		TaskQueue:                req.TaskQueue,
		Input:                    req.Input,
		WorkflowExecutionTimeout: req.WorkflowExecutionTimeout.AsDuration(),
		WorkflowRunTimeout:       req.WorkflowRunTimeout.AsDuration(),
		WorkflowTaskTimeout:      req.WorkflowTaskTimeout.AsDuration(),
		RetryPolicy:              req.RetryPolicy,
		RequestID:                requestID,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.StartWorkflowExecutionResponse{RunId: runID}, nil
}

func (s *Server) SignalWorkflowExecution(ctx context.Context, req *flowv1.SignalWorkflowExecutionRequest) (*flowv1.SignalWorkflowExecutionResponse, error) {
	if err := checkPayloadSize(req.Input); err != nil {
		return nil, err
	}
	err := s.store.SignalWorkflowExecution(ctx, history.SignalWorkflowExecutionParams{
		Namespace: namespaceOrDefault(req.Namespace), WorkflowID: req.WorkflowId, RunID: req.RunId,
		SignalName: req.SignalName, Input: req.Input,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.SignalWorkflowExecutionResponse{}, nil
}

func (s *Server) RequestCancelWorkflowExecution(ctx context.Context, req *flowv1.RequestCancelWorkflowExecutionRequest) (*flowv1.RequestCancelWorkflowExecutionResponse, error) {
	err := s.store.RequestCancelWorkflowExecution(ctx, history.RequestCancelWorkflowExecutionParams{
		Namespace: namespaceOrDefault(req.Namespace), WorkflowID: req.WorkflowId, RunID: req.RunId,
		Reason: req.Reason,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.RequestCancelWorkflowExecutionResponse{}, nil
}

func (s *Server) GetWorkflowExecutionHistory(ctx context.Context, req *flowv1.GetWorkflowExecutionHistoryRequest) (*flowv1.GetWorkflowExecutionHistoryResponse, error) {
	events, nextToken, err := s.store.GetHistory(ctx, namespaceOrDefault(req.Namespace), req.WorkflowId, req.RunId, req.PageSize, req.NextPageToken)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.GetWorkflowExecutionHistoryResponse{Events: events, NextPageToken: nextToken}, nil
}

func (s *Server) DescribeWorkflowExecution(ctx context.Context, req *flowv1.DescribeWorkflowExecutionRequest) (*flowv1.DescribeWorkflowExecutionResponse, error) {
	summary, err := s.store.DescribeWorkflowExecution(ctx, namespaceOrDefault(req.Namespace), req.WorkflowId, req.RunId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.DescribeWorkflowExecutionResponse{
		Execution:    &summary.Execution,
		WorkflowType: summary.WorkflowType,
		TaskQueue:    summary.TaskQueue,
		Status:       summary.Status,
	}, nil
}

func (s *Server) ListWorkflowExecutions(ctx context.Context, req *flowv1.ListWorkflowExecutionsRequest) (*flowv1.ListWorkflowExecutionsResponse, error) {
	infos, nextToken, err := s.store.ListWorkflowExecutions(ctx, history.ListWorkflowExecutionsParams{
		Namespace: namespaceOrDefault(req.Namespace), StatusFilter: req.StatusFilter,
		TaskQueue: req.TaskQueue, PageSize: req.PageSize, PageToken: req.NextPageToken,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	pbInfos := make([]*flowv1.WorkflowExecutionInfo, len(infos))
	for i, info := range infos {
		pbInfos[i] = info.ToProto()
	}
	return &flowv1.ListWorkflowExecutionsResponse{Executions: pbInfos, NextPageToken: nextToken}, nil
}

func (s *Server) TerminateWorkflowExecution(ctx context.Context, req *flowv1.TerminateWorkflowExecutionRequest) (*flowv1.TerminateWorkflowExecutionResponse, error) {
	if err := s.store.TerminateWorkflowExecution(ctx, namespaceOrDefault(req.Namespace), req.WorkflowId, req.RunId, req.Reason); err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.TerminateWorkflowExecutionResponse{}, nil
}

func (s *Server) PollWorkflowTaskQueue(ctx context.Context, req *flowv1.PollWorkflowTaskQueueRequest) (*flowv1.PollWorkflowTaskQueueResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultLongPollTimeout)
	defer cancel()

	task, err := matching.PollWorkflowTask(ctx, s.store, namespaceOrDefault(req.Namespace), req.TaskQueue, req.Identity, req.TaskQueuePartitions)
	if err != nil {
		if ctx.Err() != nil {
			// Long-poll timed out with nothing available: not an error, an
			// empty response tells the worker to poll again immediately.
			return &flowv1.PollWorkflowTaskQueueResponse{}, nil
		}
		return nil, toGRPCError(err)
	}
	resp := &flowv1.PollWorkflowTaskQueueResponse{
		TaskToken:              task.TaskToken,
		WorkflowExecution:      &task.Execution,
		WorkflowType:           task.WorkflowType,
		History:                task.History,
		PreviousStartedEventId: task.PreviousStartedEventID,
	}
	if task.Query != nil {
		resp.QueryTask = &flowv1.QueryTask{QueryType: task.Query.QueryType, QueryArgs: task.Query.QueryArgs}
	}
	return resp, nil
}

func (s *Server) RespondWorkflowTaskCompleted(ctx context.Context, req *flowv1.RespondWorkflowTaskCompletedRequest) (*flowv1.RespondWorkflowTaskCompletedResponse, error) {
	if err := checkCommandsPayloadSize(req.Commands); err != nil {
		return nil, err
	}
	token, err := s.store.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var sticky *history.StickyExecutionAttributes
	if a := req.StickyExecutionAttributes; a != nil {
		sticky = &history.StickyExecutionAttributes{
			WorkerIdentity:         a.WorkerIdentity,
			ScheduleToStartTimeout: a.ScheduleToStartTimeout.AsDuration(),
		}
	}
	if err := s.store.CompleteWorkflowTask(ctx, token, req.Commands, sticky); err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.RespondWorkflowTaskCompletedResponse{}, nil
}

func (s *Server) RespondWorkflowTaskFailed(ctx context.Context, req *flowv1.RespondWorkflowTaskFailedRequest) (*flowv1.RespondWorkflowTaskFailedResponse, error) {
	token, err := s.store.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.store.FailWorkflowTask(ctx, token, req.Failure); err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.RespondWorkflowTaskFailedResponse{}, nil
}

func (s *Server) PollActivityTaskQueue(ctx context.Context, req *flowv1.PollActivityTaskQueueRequest) (*flowv1.PollActivityTaskQueueResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultLongPollTimeout)
	defer cancel()

	task, err := matching.PollActivityTask(ctx, s.store, namespaceOrDefault(req.Namespace), req.TaskQueue, req.TaskQueuePartitions)
	if err != nil {
		if ctx.Err() != nil {
			return &flowv1.PollActivityTaskQueueResponse{}, nil
		}
		return nil, toGRPCError(err)
	}
	return &flowv1.PollActivityTaskQueueResponse{
		TaskToken:           task.TaskToken,
		WorkflowExecution:   &task.Execution,
		ActivityId:          task.ActivityID,
		ActivityType:        task.ActivityType,
		Input:               task.Input,
		CurrentAttempt:      task.CurrentAttempt,
		StartToCloseTimeout: durationpb.New(task.StartToCloseTimeout),
	}, nil
}

func (s *Server) RespondActivityTaskCompleted(ctx context.Context, req *flowv1.RespondActivityTaskCompletedRequest) (*flowv1.RespondActivityTaskCompletedResponse, error) {
	if err := checkPayloadSize(req.Result); err != nil {
		return nil, err
	}
	token, err := s.store.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.store.CompleteActivityTask(ctx, token, req.Result); err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.RespondActivityTaskCompletedResponse{}, nil
}

func (s *Server) RespondActivityTaskFailed(ctx context.Context, req *flowv1.RespondActivityTaskFailedRequest) (*flowv1.RespondActivityTaskFailedResponse, error) {
	token, err := s.store.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.store.FailActivityTask(ctx, token, req.Failure); err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.RespondActivityTaskFailedResponse{}, nil
}

func (s *Server) CreateNamespace(ctx context.Context, req *flowv1.CreateNamespaceRequest) (*flowv1.CreateNamespaceResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	ns, err := s.store.CreateNamespace(ctx, req.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.CreateNamespaceResponse{Namespace: namespaceInfoToProto(ns)}, nil
}

func (s *Server) ListNamespaces(ctx context.Context, _ *flowv1.ListNamespacesRequest) (*flowv1.ListNamespacesResponse, error) {
	namespaces, err := s.store.ListNamespaces(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := make([]*flowv1.NamespaceInfo, len(namespaces))
	for i, ns := range namespaces {
		out[i] = namespaceInfoToProto(ns)
	}
	return &flowv1.ListNamespacesResponse{Namespaces: out}, nil
}

func namespaceInfoToProto(ns history.NamespaceInfo) *flowv1.NamespaceInfo {
	return &flowv1.NamespaceInfo{Name: ns.Name, CreatedAt: timestamppb.New(ns.CreatedAt)}
}

func (s *Server) RespondQueryTaskCompleted(ctx context.Context, req *flowv1.RespondQueryTaskCompletedRequest) (*flowv1.RespondQueryTaskCompletedResponse, error) {
	if err := checkPayloadSize(req.Result); err != nil {
		return nil, err
	}
	token, err := s.store.DecodeTaskToken(req.TaskToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.Failure != nil {
		err = s.store.FailQuery(ctx, token, req.Failure.Message)
	} else {
		err = s.store.CompleteQuery(ctx, token, req.Result)
	}
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &flowv1.RespondQueryTaskCompletedResponse{}, nil
}

// queryPollInterval/defaultQueryTimeout bound QueryWorkflowExecution's own
// wait for a worker to answer — short and synchronous, unlike the
// long-poll RPCs: the caller here is a client blocked on a single RPC, not
// a worker's poll loop retrying indefinitely.
const (
	queryPollInterval   = 50 * time.Millisecond
	defaultQueryTimeout = 10 * time.Second
)

func (s *Server) QueryWorkflowExecution(ctx context.Context, req *flowv1.QueryWorkflowExecutionRequest) (*flowv1.QueryWorkflowExecutionResponse, error) {
	if err := checkPayloadSize(req.QueryArgs); err != nil {
		return nil, err
	}
	if req.QueryType == "" {
		return nil, status.Error(codes.InvalidArgument, "query_type is required")
	}

	nsID, taskID, err := s.store.EnqueueQuery(ctx, history.EnqueueQueryParams{
		Namespace: namespaceOrDefault(req.Namespace), WorkflowID: req.WorkflowId, RunID: req.RunId,
		QueryType: req.QueryType, QueryArgs: req.QueryArgs,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	ticker := time.NewTicker(queryPollInterval)
	defer ticker.Stop()

	for {
		result, err := s.store.GetQueryResult(ctx, nsID, taskID)
		if err != nil {
			return nil, toGRPCError(err)
		}
		switch result.Status {
		case "COMPLETED":
			if err := s.store.DeleteQueryTask(ctx, taskID); err != nil {
				s.logger.Error("delete completed query task", "error", err)
			}
			return &flowv1.QueryWorkflowExecutionResponse{Result: result.Result}, nil
		case "FAILED":
			if err := s.store.DeleteQueryTask(ctx, taskID); err != nil {
				s.logger.Error("delete failed query task", "error", err)
			}
			return nil, status.Error(codes.Internal, result.FailureMessage)
		}
		select {
		case <-ctx.Done():
			// Give up: delete the row so a worker that answers later (its
			// own lease/reap cycle, unaware this caller stopped watching)
			// doesn't leave it behind forever. Best-effort — if a worker
			// is completing it at this exact moment, whichever write wins
			// is already a modeled outcome (CompleteQuery/FailQuery treat
			// a vanished row as ErrTaskLeaseExpired).
			if err := s.store.DeleteQueryTask(context.WithoutCancel(ctx), taskID); err != nil {
				s.logger.Error("delete abandoned query task", "error", err)
			}
			return nil, status.Error(codes.DeadlineExceeded, "query timed out waiting for a worker to answer")
		case <-ticker.C:
		}
	}
}
