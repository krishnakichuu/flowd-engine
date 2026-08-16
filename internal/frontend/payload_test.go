package frontend

import (
	"context"
	"io"
	"log/slog"
	"testing"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func bytesOfLen(n int) []byte {
	return make([]byte, n)
}

func TestCheckPayloadSize(t *testing.T) {
	tests := []struct {
		name    string
		payload *flowv1.Payload
		wantErr bool
	}{
		{"nil payload", nil, false},
		{"empty data", &flowv1.Payload{}, false},
		{"well under the cap", &flowv1.Payload{Data: bytesOfLen(1024)}, false},
		{"exactly at the cap", &flowv1.Payload{Data: bytesOfLen(maxPayloadBytes)}, false},
		{"one byte over the cap", &flowv1.Payload{Data: bytesOfLen(maxPayloadBytes + 1)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkPayloadSize(tt.payload)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("got code %v, want InvalidArgument", got)
			}
		})
	}
}

func TestCheckCommandsPayloadSize(t *testing.T) {
	oversized := bytesOfLen(maxPayloadBytes + 1)

	t.Run("no commands", func(t *testing.T) {
		if err := checkCommandsPayloadSize(nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("commands without a payload are never flagged", func(t *testing.T) {
		commands := []*flowv1.Command{
			{Command: &flowv1.Command_StartTimer{StartTimer: &flowv1.StartTimerCommand{TimerId: 1, Duration: durationpb.New(0)}}},
			{Command: &flowv1.Command_FailWorkflowExecution{FailWorkflowExecution: &flowv1.FailWorkflowExecutionCommand{Failure: &flowv1.Failure{Message: "boom"}}}},
		}
		if err := checkCommandsPayloadSize(commands); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("oversized ScheduleActivityTask input is caught", func(t *testing.T) {
		commands := []*flowv1.Command{
			{Command: &flowv1.Command_ScheduleActivityTask{ScheduleActivityTask: &flowv1.ScheduleActivityTaskCommand{
				ActivityId: 1, ActivityType: "Big", Input: &flowv1.Payload{Data: oversized},
			}}},
		}
		err := checkCommandsPayloadSize(commands)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", err)
		}
	})

	t.Run("oversized CompleteWorkflowExecution result is caught", func(t *testing.T) {
		commands := []*flowv1.Command{
			{Command: &flowv1.Command_CompleteWorkflowExecution{CompleteWorkflowExecution: &flowv1.CompleteWorkflowExecutionCommand{
				Result: &flowv1.Payload{Data: oversized},
			}}},
		}
		err := checkCommandsPayloadSize(commands)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", err)
		}
	})

	t.Run("oversized ContinueAsNew input is caught", func(t *testing.T) {
		commands := []*flowv1.Command{
			{Command: &flowv1.Command_ContinueAsNewWorkflowExecution{ContinueAsNewWorkflowExecution: &flowv1.ContinueAsNewWorkflowExecutionCommand{
				WorkflowType: "Loop", Input: &flowv1.Payload{Data: oversized},
			}}},
		}
		err := checkCommandsPayloadSize(commands)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", err)
		}
	})

	t.Run("a valid command ahead of the oversized one doesn't hide it", func(t *testing.T) {
		commands := []*flowv1.Command{
			{Command: &flowv1.Command_StartTimer{StartTimer: &flowv1.StartTimerCommand{TimerId: 1}}},
			{Command: &flowv1.Command_ScheduleActivityTask{ScheduleActivityTask: &flowv1.ScheduleActivityTaskCommand{
				Input: &flowv1.Payload{Data: oversized},
			}}},
		}
		if err := checkCommandsPayloadSize(commands); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", err)
		}
	})
}

// The four tests below prove the cap is enforced before any store access:
// each Server has a nil *history.Store, so a nil-pointer panic would occur
// immediately if the handler tried to use it — reaching a clean
// InvalidArgument response instead proves checkPayloadSize/
// checkCommandsPayloadSize runs first.

func newTestServer() *Server {
	return NewServer(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestStartWorkflowExecutionRejectsOversizedInput(t *testing.T) {
	s := newTestServer()
	_, err := s.StartWorkflowExecution(context.Background(), &flowv1.StartWorkflowExecutionRequest{
		WorkflowId: "wf-1", WorkflowType: "T", TaskQueue: "q",
		Input: &flowv1.Payload{Data: bytesOfLen(maxPayloadBytes + 1)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestSignalWorkflowExecutionRejectsOversizedInput(t *testing.T) {
	s := newTestServer()
	_, err := s.SignalWorkflowExecution(context.Background(), &flowv1.SignalWorkflowExecutionRequest{
		WorkflowId: "wf-1", SignalName: "sig",
		Input: &flowv1.Payload{Data: bytesOfLen(maxPayloadBytes + 1)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestRespondActivityTaskCompletedRejectsOversizedResult(t *testing.T) {
	s := newTestServer()
	_, err := s.RespondActivityTaskCompleted(context.Background(), &flowv1.RespondActivityTaskCompletedRequest{
		TaskToken: []byte("not-a-real-token"),
		Result:    &flowv1.Payload{Data: bytesOfLen(maxPayloadBytes + 1)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestRespondWorkflowTaskCompletedRejectsOversizedCommandPayload(t *testing.T) {
	s := newTestServer()
	_, err := s.RespondWorkflowTaskCompleted(context.Background(), &flowv1.RespondWorkflowTaskCompletedRequest{
		TaskToken: []byte("not-a-real-token"),
		Commands: []*flowv1.Command{
			{Command: &flowv1.Command_CompleteWorkflowExecution{CompleteWorkflowExecution: &flowv1.CompleteWorkflowExecutionCommand{
				Result: &flowv1.Payload{Data: bytesOfLen(maxPayloadBytes + 1)},
			}}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}
