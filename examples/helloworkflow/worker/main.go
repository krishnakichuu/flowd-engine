// Command helloworkflow-worker runs a worker for the helloworkflow example.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/krishnakichuu/flowd/examples/helloworkflow"
	"github.com/krishnakichuu/flowd/sdk/client"
	"github.com/krishnakichuu/flowd/sdk/worker"
)

func main() {
	target := os.Getenv("FLOWD_ADDR")
	if target == "" {
		target = "localhost:7233"
	}

	c, err := client.Dial(target, client.Options{})
	if err != nil {
		log.Fatalf("dial flowd: %v", err)
	}
	defer func() { _ = c.Close() }()

	w := worker.New(c, helloworkflow.TaskQueue, worker.Options{})
	w.RegisterWorkflow(helloworkflow.SimpleWorkflow)
	w.RegisterActivity(helloworkflow.GreetActivity)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// #nosec G706 -- target/TaskQueue come from a local env var and a
	// compile-time constant, not untrusted input; this is a startup log line.
	log.Printf("helloworkflow worker polling task queue %q at %s", helloworkflow.TaskQueue, target)
	w.Run(ctx)
}
