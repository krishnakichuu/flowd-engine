package frontend

import (
	"fmt"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxPayloadBytes is the cap common.proto's Payload message documents on
// Payload.data — "Phase 1 enforces a 2MB soft cap on `data` at the API
// layer" — but until now nothing actually checked it (see Phase 2 roadmap,
// Track A, item 5), so a single oversized input/result could grow that
// run's history_events without bound.
const maxPayloadBytes = 2 * 1024 * 1024 // 2MB

// checkPayloadSize rejects p if its data exceeds maxPayloadBytes. A nil
// Payload (the field wasn't set) is not this check's problem.
func checkPayloadSize(p *flowv1.Payload) error {
	if p == nil || len(p.GetData()) <= maxPayloadBytes {
		return nil
	}
	return status.Error(codes.InvalidArgument, fmt.Sprintf(
		"payload exceeds the %d byte limit (got %d bytes)", maxPayloadBytes, len(p.GetData()),
	))
}

// checkCommandsPayloadSize checks every Payload a RespondWorkflowTaskCompleted
// call can carry. Of Command's five variants, only ScheduleActivityTaskCommand
// (Input), CompleteWorkflowExecutionCommand (Result), and
// ContinueAsNewWorkflowExecutionCommand (Input) carry one — StartTimer and
// FailWorkflowExecution don't.
func checkCommandsPayloadSize(commands []*flowv1.Command) error {
	for _, c := range commands {
		switch v := c.GetCommand().(type) {
		case *flowv1.Command_ScheduleActivityTask:
			if err := checkPayloadSize(v.ScheduleActivityTask.GetInput()); err != nil {
				return err
			}
		case *flowv1.Command_CompleteWorkflowExecution:
			if err := checkPayloadSize(v.CompleteWorkflowExecution.GetResult()); err != nil {
				return err
			}
		case *flowv1.Command_ContinueAsNewWorkflowExecution:
			if err := checkPayloadSize(v.ContinueAsNewWorkflowExecution.GetInput()); err != nil {
				return err
			}
		}
	}
	return nil
}
