// Command countdown-starter starts one CountdownWorkflow execution and
// waits for its final result, following the execution transparently
// through however many continue-as-new runs it takes to get there.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/krishnakichuu/flowd/examples/countdown"
	"github.com/krishnakichuu/flowd/sdk/client"
)

func main() {
	remaining := flag.Int("count", 3, "number of ticks before completing")
	workflowID := flag.String("id", "countdown-"+time.Now().Format("20060102-150405"), "workflow ID")
	flag.Parse()

	target := os.Getenv("FLOWD_ADDR")
	if target == "" {
		target = "localhost:7233"
	}

	c, err := client.Dial(target, client.Options{})
	if err != nil {
		log.Fatalf("dial flowd: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	run, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{
		ID:        *workflowID,
		TaskQueue: countdown.TaskQueue,
	}, countdown.CountdownWorkflow, *remaining)
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}
	fmt.Printf("started workflow_id=%s run_id=%s\n", run.WorkflowID, run.RunID)

	var result string
	if err := run.Get(ctx, &result); err != nil {
		log.Fatalf("workflow did not complete: %v", err)
	}
	fmt.Printf("%s (final run_id=%s)\n", result, run.RunID)
}
