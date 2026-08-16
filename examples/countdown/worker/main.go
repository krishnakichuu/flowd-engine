// Command countdown-worker runs a worker for the countdown example.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/krishnakichuu/flowd/examples/countdown"
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

	w := worker.New(c, countdown.TaskQueue, worker.Options{})
	w.RegisterWorkflow(countdown.CountdownWorkflow)
	w.RegisterActivity(countdown.TickActivity)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// #nosec G706 -- target/TaskQueue come from a local env var and a
	// compile-time constant, not untrusted input; this is a startup log line.
	log.Printf("countdown worker polling task queue %q at %s", countdown.TaskQueue, target)
	w.Run(ctx)
}
