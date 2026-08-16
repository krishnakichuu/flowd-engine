// Command flow-cli is a thin operator CLI wrapping sdk/client: start,
// signal, describe, and inspect workflow history against a running flowd
// server. It is not the primary way applications integrate with flowd
// (that's the SDK) — it exists for the quickstart and day-to-day ops use.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/client"
)

// version is set at build time via -ldflags "-X main.version=...", e.g. by
// .goreleaser.yml. Left as "dev" for local/non-release builds.
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	if os.Args[1] == "version" || os.Args[1] == "-version" || os.Args[1] == "--version" {
		fmt.Println(version)
		return
	}

	target := os.Getenv("FLOWD_ADDR")
	if target == "" {
		target = "localhost:7233"
	}
	c, err := client.Dial(target, client.Options{TLS: tlsOptionsFromEnv(), APIKey: os.Getenv("FLOWD_API_KEY")})
	if err != nil {
		fatalf("dial %s: %v", target, err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	switch os.Args[1] {
	case "start":
		cmdStart(ctx, c, os.Args[2:])
	case "signal":
		cmdSignal(ctx, c, os.Args[2:])
	case "cancel":
		cmdCancel(ctx, c, os.Args[2:])
	case "list":
		cmdList(ctx, c, os.Args[2:])
	case "describe":
		cmdDescribe(ctx, c, os.Args[2:])
	case "history":
		cmdHistory(ctx, c, os.Args[2:])
	case "query":
		cmdQuery(ctx, c, os.Args[2:])
	case "namespace-create":
		cmdNamespaceCreate(ctx, c, os.Args[2:])
	case "namespace-list":
		cmdNamespaceList(ctx, c)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: flow-cli <start|list|signal|cancel|describe|history|query|namespace-create|namespace-list|version> [flags]

Set FLOWD_ADDR to target a non-default server (default localhost:7233).
Set FLOWD_API_KEY to authenticate against a server with FLOWD_API_KEYS configured.
Set FLOWD_TLS_CA_FILE to dial over TLS (required against a server with FLOWD_TLS_CERT_FILE configured);
optionally FLOWD_TLS_CLIENT_CERT_FILE/FLOWD_TLS_CLIENT_KEY_FILE for mTLS and FLOWD_TLS_SERVER_NAME to
override the name used for certificate verification.`)
}

// tlsOptionsFromEnv returns nil (plaintext, flow-cli's historical default)
// unless FLOWD_TLS_CA_FILE is set.
func tlsOptionsFromEnv() *client.TLSOptions {
	caFile := os.Getenv("FLOWD_TLS_CA_FILE")
	if caFile == "" {
		return nil
	}
	return &client.TLSOptions{
		CACertFile: caFile,
		CertFile:   os.Getenv("FLOWD_TLS_CLIENT_CERT_FILE"),
		KeyFile:    os.Getenv("FLOWD_TLS_CLIENT_KEY_FILE"),
		ServerName: os.Getenv("FLOWD_TLS_SERVER_NAME"),
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func cmdStart(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	id := fs.String("id", "", "workflow ID (required)")
	workflowType := fs.String("type", "", "workflow type name (required)")
	taskQueue := fs.String("task-queue", "", "task queue (required)")
	input := fs.String("input", "null", "JSON input")
	_ = fs.Parse(args)

	if *id == "" || *workflowType == "" || *taskQueue == "" {
		fatalf("start: -id, -type, and -task-queue are required")
	}
	var arg any
	if err := json.Unmarshal([]byte(*input), &arg); err != nil {
		fatalf("invalid -input JSON: %v", err)
	}

	run, err := c.StartWorkflowByType(ctx, client.StartWorkflowOptions{ID: *id, TaskQueue: *taskQueue}, *workflowType, arg)
	if err != nil {
		fatalf("start workflow: %v", err)
	}
	fmt.Printf("workflow_id=%s run_id=%s\n", run.WorkflowID, run.RunID)
}

func cmdSignal(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("signal", flag.ExitOnError)
	id := fs.String("id", "", "workflow ID (required)")
	runID := fs.String("run-id", "", "run ID (defaults to current run)")
	name := fs.String("name", "", "signal name (required)")
	input := fs.String("input", "null", "JSON input")
	_ = fs.Parse(args)

	if *id == "" || *name == "" {
		fatalf("signal: -id and -name are required")
	}
	var arg any
	if err := json.Unmarshal([]byte(*input), &arg); err != nil {
		fatalf("invalid -input JSON: %v", err)
	}
	if err := c.SignalWorkflow(ctx, *id, *runID, *name, arg); err != nil {
		fatalf("signal workflow: %v", err)
	}
	fmt.Println("signaled")
}

func cmdCancel(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	id := fs.String("id", "", "workflow ID (required)")
	runID := fs.String("run-id", "", "run ID (defaults to current run)")
	reason := fs.String("reason", "", "reason, visible to the workflow via workflow.CancelReason")
	_ = fs.Parse(args)

	if *id == "" {
		fatalf("cancel: -id is required")
	}
	if err := c.CancelWorkflow(ctx, *id, *runID, *reason); err != nil {
		fatalf("cancel workflow: %v", err)
	}
	fmt.Println("cancel requested")
}

func cmdList(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	status := fs.String("status", "", "filter by status: running|completed|failed|terminated|timed_out|continued_as_new (default: all)")
	taskQueue := fs.String("task-queue", "", "filter by task queue (default: all)")
	pageSize := fs.Int("page-size", 0, "page size (default: server default)")
	pageToken := fs.String("page-token", "", "opaque page token from a previous list's next_page_token")
	_ = fs.Parse(args)

	statusFilter, err := parseStatusFilter(*status)
	if err != nil {
		fatalf("list: %v", err)
	}

	resp, err := c.ListWorkflows(ctx, client.ListWorkflowsOptions{
		StatusFilter: statusFilter, TaskQueue: *taskQueue,
		PageSize: int32(*pageSize), PageToken: []byte(*pageToken),
	})
	if err != nil {
		fatalf("list workflows: %v", err)
	}
	for _, e := range resp.Executions {
		closeTime := "-"
		if e.CloseTime != nil {
			closeTime = e.CloseTime.AsTime().Format("2006-01-02T15:04:05Z07:00")
		}
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			e.Execution.WorkflowId, e.Execution.RunId, e.WorkflowType, e.TaskQueue,
			e.Status.String(), e.StartTime.AsTime().Format("2006-01-02T15:04:05Z07:00"), closeTime)
	}
	if len(resp.NextPageToken) > 0 {
		fmt.Fprintf(os.Stderr, "\n-page-token %q for more\n", string(resp.NextPageToken))
	}
}

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
		return flowv1.WorkflowExecutionStatus_WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, fmt.Errorf("unknown -status %q", s)
	}
}

func cmdDescribe(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("describe", flag.ExitOnError)
	id := fs.String("id", "", "workflow ID (required)")
	runID := fs.String("run-id", "", "run ID (defaults to current run)")
	_ = fs.Parse(args)

	if *id == "" {
		fatalf("describe: -id is required")
	}
	resp, err := c.DescribeWorkflow(ctx, *id, *runID)
	if err != nil {
		fatalf("describe workflow: %v", err)
	}
	out, _ := json.MarshalIndent(map[string]any{
		"workflow_id":   resp.Execution.WorkflowId,
		"run_id":        resp.Execution.RunId,
		"workflow_type": resp.WorkflowType,
		"task_queue":    resp.TaskQueue,
		"status":        resp.Status.String(),
	}, "", "  ")
	fmt.Println(string(out))
}

func cmdHistory(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	id := fs.String("id", "", "workflow ID (required)")
	runID := fs.String("run-id", "", "run ID (required)")
	_ = fs.Parse(args)

	if *id == "" || *runID == "" {
		fatalf("history: -id and -run-id are required")
	}
	events, err := c.GetWorkflowHistory(ctx, *id, *runID)
	if err != nil {
		fatalf("get history: %v", err)
	}
	for _, ev := range events {
		fmt.Printf("%d\t%s\t%s\n", ev.EventId, ev.EventType, ev.EventTime.AsTime().Format("2006-01-02T15:04:05.000Z07:00"))
	}
}

func cmdQuery(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	id := fs.String("id", "", "workflow ID (required)")
	runID := fs.String("run-id", "", "run ID (defaults to current run)")
	queryType := fs.String("type", "", "query type (required, matches a workflow.SetQueryHandler registration)")
	input := fs.String("input", "null", "JSON query args")
	_ = fs.Parse(args)

	if *id == "" || *queryType == "" {
		fatalf("query: -id and -type are required")
	}
	var arg any
	if err := json.Unmarshal([]byte(*input), &arg); err != nil {
		fatalf("invalid -input JSON: %v", err)
	}
	var result json.RawMessage
	if err := c.QueryWorkflow(ctx, *id, *runID, *queryType, arg, &result); err != nil {
		fatalf("query workflow: %v", err)
	}
	fmt.Println(string(result))
}

func cmdNamespaceCreate(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("namespace-create", flag.ExitOnError)
	name := fs.String("name", "", "namespace name (required)")
	_ = fs.Parse(args)

	if *name == "" {
		fatalf("namespace-create: -name is required")
	}
	ns, err := c.CreateNamespace(ctx, *name)
	if err != nil {
		fatalf("create namespace: %v", err)
	}
	fmt.Printf("created namespace=%s created_at=%s\n", ns.Name, ns.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"))
}

func cmdNamespaceList(ctx context.Context, c *client.Client) {
	namespaces, err := c.ListNamespaces(ctx)
	if err != nil {
		fatalf("list namespaces: %v", err)
	}
	for _, ns := range namespaces {
		fmt.Printf("%s\t%s\n", ns.Name, ns.CreatedAt.AsTime().Format("2006-01-02T15:04:05Z07:00"))
	}
}
