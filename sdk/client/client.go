// Package client provides the Go client for starting, signaling, and
// inspecting flowd workflow executions.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
	"github.com/krishnakichuu/flowd/sdk/internal/funcname"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Client struct {
	conn      *grpc.ClientConn
	rpc       flowv1.WorkflowServiceClient
	namespace string
}

type Options struct {
	// Namespace defaults to "default".
	Namespace string

	// TLS enables transport security when set; nil (the default) dials
	// plaintext, matching Phase 1's behavior. Required against any server
	// with TLSCertFile/TLSKeyFile configured (see internal/config).
	TLS *TLSOptions

	// APIKey, if set, is attached to every outgoing RPC as the
	// x-flowd-api-key metadata header, matched against a server started
	// with FLOWD_API_KEYS configured. Sending this over a plaintext
	// (TLS == nil) connection leaks the key on the wire — pair it with TLS
	// outside a fully trusted network.
	APIKey string
}

// TLSOptions configures the client's gRPC transport credentials.
type TLSOptions struct {
	// CACertFile is a PEM file used to verify the server's certificate.
	// Empty uses the host's system certificate pool.
	CACertFile string
	// CertFile and KeyFile are a PEM client certificate/key pair, required
	// only when dialing a server configured for mTLS (TLSClientCAFile).
	CertFile string
	KeyFile  string
	// ServerName overrides the name used for SNI and certificate
	// verification, e.g. when target is an IP address.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. Dev-only
	// escape hatch; never set this against an untrusted network.
	InsecureSkipVerify bool
}

// Dial connects to a flowd server at target (e.g. "localhost:7233").
func Dial(target string, opts Options) (*Client, error) {
	creds, err := transportCredentials(opts.TLS)
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if opts.APIKey != "" {
		dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(apiKeyUnaryClientInterceptor(opts.APIKey)))
	}
	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, err
	}
	ns := opts.Namespace
	if ns == "" {
		ns = "default"
	}
	return &Client{conn: conn, rpc: flowv1.NewWorkflowServiceClient(conn), namespace: ns}, nil
}

func transportCredentials(opts *TLSOptions) (credentials.TransportCredentials, error) {
	if opts == nil {
		return insecure.NewCredentials(), nil
	}
	tlsCfg := &tls.Config{
		ServerName:         opts.ServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // explicit opt-in via TLSOptions
	}
	if opts.CACertFile != "" {
		pem, err := os.ReadFile(opts.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse CA cert %s: no valid certificates found", opts.CACertFile)
		}
		tlsCfg.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

// apiKeyUnaryClientInterceptor attaches key to every outgoing RPC so a
// server-side auth interceptor (internal/frontend.NewAPIKeyUnaryInterceptor)
// can authenticate it.
func apiKeyUnaryClientInterceptor(key string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-flowd-api-key", key)
		return invoker(ctx, method, req, reply, cc, callOpts...)
	}
}

func (c *Client) Close() error { return c.conn.Close() }

// WorkflowService exposes the raw generated gRPC client for sdk/worker's
// use when polling task queues; workflow authors should use the
// higher-level methods above instead.
func (c *Client) WorkflowService() flowv1.WorkflowServiceClient { return c.rpc }

// Namespace returns the namespace this client was configured with.
func (c *Client) Namespace() string { return c.namespace }

type StartWorkflowOptions struct {
	ID                       string
	TaskQueue                string
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	RetryPolicy              *flowv1.RetryPolicy
	// RequestID is the idempotency key; if empty, StartWorkflow generates
	// one, meaning repeated calls are NOT automatically idempotent unless
	// the caller supplies a stable RequestID itself.
	RequestID string
}

// WorkflowRun identifies a started execution and lets a caller wait for and
// unmarshal its result.
type WorkflowRun struct {
	client     *Client
	WorkflowID string
	RunID      string
}

// StartWorkflow starts workflowFn (a reference to a registered workflow
// function) with a single input argument, following the
// func(workflow.Context, In) (Out, error) convention.
func (c *Client) StartWorkflow(ctx context.Context, opts StartWorkflowOptions, workflowFn any, arg any) (*WorkflowRun, error) {
	return c.StartWorkflowByType(ctx, opts, funcname.Of(workflowFn), arg)
}

// StartWorkflowByType is StartWorkflow's lower-level form for callers that
// only have the workflow type name as a string (e.g. flow-cli), not a
// reference to the Go function.
func (c *Client) StartWorkflowByType(ctx context.Context, opts StartWorkflowOptions, workflowType string, arg any) (*WorkflowRun, error) {
	payload, err := converter.Default.ToPayload(arg)
	if err != nil {
		return nil, err
	}
	resp, err := c.rpc.StartWorkflowExecution(ctx, &flowv1.StartWorkflowExecutionRequest{
		Namespace:                c.namespace,
		WorkflowId:               opts.ID,
		WorkflowType:             workflowType,
		TaskQueue:                opts.TaskQueue,
		Input:                    payload,
		WorkflowExecutionTimeout: durationpbOrNil(opts.WorkflowExecutionTimeout),
		WorkflowRunTimeout:       durationpbOrNil(opts.WorkflowRunTimeout),
		WorkflowTaskTimeout:      durationpbOrNil(opts.WorkflowTaskTimeout),
		RetryPolicy:              opts.RetryPolicy,
		RequestId:                opts.RequestID,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowRun{client: c, WorkflowID: opts.ID, RunID: resp.RunId}, nil
}

// Get polls until the run reaches a terminal status and unmarshals a
// successful result into resultPtr (pass nil to just wait for completion).
// This is deliberately simple polling rather than a long-poll/streaming
// API, matching Phase 1's scope.
//
// A run that continued as new is not "the workflow is done" from a
// caller's point of view — it's followed transparently: Get re-resolves
// the workflow_id's current run (an empty run_id) and keeps polling, all
// the way through any number of further continuations, until it reaches an
// actually-terminal status. r.RunID is updated to whichever run Get
// ultimately settles on, so it's correct to inspect afterward.
func (r *WorkflowRun) Get(ctx context.Context, resultPtr any) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	runID := r.RunID
	for {
		desc, err := r.client.DescribeWorkflow(ctx, r.WorkflowID, runID)
		if err != nil {
			return err
		}
		switch desc.Status {
		case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_COMPLETED:
			r.RunID = desc.Execution.RunId
			return r.fetchResult(ctx, resultPtr)
		case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_FAILED,
			flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TERMINATED,
			flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
			r.RunID = desc.Execution.RunId
			return fmt.Errorf("client: workflow did not complete successfully: status %s", desc.Status)
		case flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
			// runID's own status can never change again — re-resolve via
			// current_executions instead of polling a dead end.
			runID = ""
			continue
		}
		runID = desc.Execution.RunId
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *WorkflowRun) fetchResult(ctx context.Context, resultPtr any) error {
	events, err := r.client.GetWorkflowHistory(ctx, r.WorkflowID, r.RunID)
	if err != nil {
		return err
	}
	for i := len(events) - 1; i >= 0; i-- {
		if a, ok := events[i].Attributes.(*flowv1.HistoryEvent_WorkflowExecutionCompletedEventAttributes); ok {
			if resultPtr == nil {
				return nil
			}
			return converter.Default.FromPayload(a.WorkflowExecutionCompletedEventAttributes.Result, resultPtr)
		}
	}
	return fmt.Errorf("client: workflow completed but no WorkflowExecutionCompleted event found")
}

func (c *Client) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error {
	payload, err := converter.Default.ToPayload(arg)
	if err != nil {
		return err
	}
	_, err = c.rpc.SignalWorkflowExecution(ctx, &flowv1.SignalWorkflowExecutionRequest{
		Namespace: c.namespace, WorkflowId: workflowID, RunId: runID, SignalName: signalName, Input: payload,
	})
	return err
}

// QueryWorkflow synchronously asks a running (or completed) execution's
// query_type handler — registered in workflow code via
// workflow.SetQueryHandler — for its current answer, and unmarshals the
// result into resultPtr. Unlike SignalWorkflow, this blocks until a
// worker actually answers or the server's own bounded wait gives up; it
// never appends anything to the workflow's history.
func (c *Client) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, arg any, resultPtr any) error {
	payload, err := converter.Default.ToPayload(arg)
	if err != nil {
		return err
	}
	resp, err := c.rpc.QueryWorkflowExecution(ctx, &flowv1.QueryWorkflowExecutionRequest{
		Namespace: c.namespace, WorkflowId: workflowID, RunId: runID, QueryType: queryType, QueryArgs: payload,
	})
	if err != nil {
		return err
	}
	if resultPtr == nil {
		return nil
	}
	return converter.Default.FromPayload(resp.Result, resultPtr)
}

func (c *Client) DescribeWorkflow(ctx context.Context, workflowID, runID string) (*flowv1.DescribeWorkflowExecutionResponse, error) {
	return c.rpc.DescribeWorkflowExecution(ctx, &flowv1.DescribeWorkflowExecutionRequest{
		Namespace: c.namespace, WorkflowId: workflowID, RunId: runID,
	})
}

// ListWorkflowsOptions filters/paginates ListWorkflows. StatusFilter left
// at its zero value (UNSPECIFIED) matches every status; TaskQueue left
// empty matches every task queue; PageSize left at zero uses the server's
// default.
type ListWorkflowsOptions struct {
	StatusFilter flowv1.WorkflowExecutionStatus
	TaskQueue    string
	PageSize     int32
	PageToken    []byte
}

// ListWorkflowsResult is one page of ListWorkflows — unlike
// GetWorkflowHistory, this does not auto-paginate to completion: callers
// (flow-cli, the web UI) want to control paging themselves rather than
// pull a namespace's entire execution history into memory at once.
type ListWorkflowsResult struct {
	Executions    []*flowv1.WorkflowExecutionInfo
	NextPageToken []byte
}

// ListWorkflows returns one page of workflow executions in this client's
// namespace, newest-started first — the primitive for discovering
// workflow IDs, which every other Client method requires already knowing.
func (c *Client) ListWorkflows(ctx context.Context, opts ListWorkflowsOptions) (*ListWorkflowsResult, error) {
	resp, err := c.rpc.ListWorkflowExecutions(ctx, &flowv1.ListWorkflowExecutionsRequest{
		Namespace: c.namespace, StatusFilter: opts.StatusFilter, TaskQueue: opts.TaskQueue,
		PageSize: opts.PageSize, NextPageToken: opts.PageToken,
	})
	if err != nil {
		return nil, err
	}
	return &ListWorkflowsResult{Executions: resp.Executions, NextPageToken: resp.NextPageToken}, nil
}

func (c *Client) GetWorkflowHistory(ctx context.Context, workflowID, runID string) ([]*flowv1.HistoryEvent, error) {
	var events []*flowv1.HistoryEvent
	var token []byte
	for {
		resp, err := c.rpc.GetWorkflowExecutionHistory(ctx, &flowv1.GetWorkflowExecutionHistoryRequest{
			Namespace: c.namespace, WorkflowId: workflowID, RunId: runID, NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, resp.Events...)
		if len(resp.NextPageToken) == 0 {
			return events, nil
		}
		token = resp.NextPageToken
	}
}

func (c *Client) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string) error {
	_, err := c.rpc.TerminateWorkflowExecution(ctx, &flowv1.TerminateWorkflowExecutionRequest{
		Namespace: c.namespace, WorkflowId: workflowID, RunId: runID, Reason: reason,
	})
	return err
}

// CancelWorkflow asks a running workflow to wind down gracefully — unlike
// TerminateWorkflow, this does not close the run itself; it wakes the
// workflow with a request its own code observes via
// workflow.IsCancelRequested and decides how to act on.
func (c *Client) CancelWorkflow(ctx context.Context, workflowID, runID, reason string) error {
	_, err := c.rpc.RequestCancelWorkflowExecution(ctx, &flowv1.RequestCancelWorkflowExecutionRequest{
		Namespace: c.namespace, WorkflowId: workflowID, RunId: runID, Reason: reason,
	})
	return err
}

// CreateNamespace creates a new namespace. Unlike every other Client
// method, this doesn't act within c's own namespace — namespaces
// themselves are what's being managed.
func (c *Client) CreateNamespace(ctx context.Context, name string) (*flowv1.NamespaceInfo, error) {
	resp, err := c.rpc.CreateNamespace(ctx, &flowv1.CreateNamespaceRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return resp.Namespace, nil
}

// ListNamespaces returns every namespace on the server, ordered by name.
func (c *Client) ListNamespaces(ctx context.Context) ([]*flowv1.NamespaceInfo, error) {
	resp, err := c.rpc.ListNamespaces(ctx, &flowv1.ListNamespacesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Namespaces, nil
}

func durationpbOrNil(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}
	return durationpb.New(d)
}
