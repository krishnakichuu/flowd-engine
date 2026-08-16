// Command helloworkflow-starter starts one SimpleWorkflow execution and
// waits for its result — the Phase 1 quickstart's "start a workflow" step.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/krishnakichuu/flowd/examples/helloworkflow"
	"github.com/krishnakichuu/flowd/sdk/client"
)

func main() {
	name := flag.String("name", "World", "name to greet")
	workflowID := flag.String("id", "helloworkflow-"+time.Now().Format("20060102-150405"), "workflow ID")
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
		TaskQueue: helloworkflow.TaskQueue,
	}, helloworkflow.SimpleWorkflow, *name)
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}
	fmt.Printf("started workflow_id=%s run_id=%s\n", run.WorkflowID, run.RunID)

	var result string
	if err := run.Get(ctx, &result); err != nil {
		log.Fatalf("workflow did not complete: %v", err)
	}
	fmt.Println(result)
}
