// Package replay holds the SDK's golden-replay safety net: it replays a
// checked-in, real history fixture against the current workflow code and
// asserts the result still matches, and separately asserts that a
// workflow mutated to schedule a different activity at the same position
// is caught as non-deterministic rather than silently misbehaving (see
// ADR-0001). Unlike test/integration, these tests need no server or
// Postgres — worker.ReplayWorkflowHistory drives the replayer directly.
package replay

import (
	"encoding/json"
	"os"
	"testing"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/examples/helloworkflow"
	"github.com/krishnakichuu/flowd/sdk/activity"
	"github.com/krishnakichuu/flowd/sdk/worker"
	"github.com/krishnakichuu/flowd/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

const goldenFixture = "../../sdk/testdata/replay/simpleworkflow/v1.history.json"

func loadFixture(t *testing.T, path string) []*flowv1.HistoryEvent {
	t.Helper()
	// #nosec G304 -- path is a hardcoded test fixture constant, not
	// external input.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	events := make([]*flowv1.HistoryEvent, len(raws))
	for i, r := range raws {
		ev := &flowv1.HistoryEvent{}
		if err := protojson.Unmarshal(r, ev); err != nil {
			t.Fatalf("unmarshal event %d: %v", i, err)
		}
		events[i] = ev
	}
	return events
}

func TestReplaySimpleWorkflowGolden(t *testing.T) {
	events := loadFixture(t, goldenFixture)

	result, err := worker.ReplayWorkflowHistory(events, helloworkflow.SimpleWorkflow)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	var got string
	if err := json.Unmarshal(result.Data, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if want := "Hello, ReplayFixture!"; got != want {
		t.Fatalf("replay result = %q, want %q", got, want)
	}
}

// mutatedWorkflow schedules a different activity than GreetActivity, the
// one recorded at this position in the golden fixture — simulating a
// workflow-code change that is incompatible with an in-flight execution's
// history.
func mutatedWorkflow(ctx workflow.Context, name string) (string, error) {
	var out string
	err := workflow.ExecuteActivity(ctx, mutatedActivity, name, workflow.ActivityOptions{}).Get(&out)
	return out, err
}

func mutatedActivity(ctx activity.Context, name string) (string, error) {
	return "should never run", nil
}

func TestReplayDetectsNonDeterminism(t *testing.T) {
	events := loadFixture(t, goldenFixture)

	_, err := worker.ReplayWorkflowHistory(events, mutatedWorkflow)
	if err == nil {
		t.Fatal("expected a non-determinism error, got nil")
	}
	if !worker.IsNonDeterministic(err) {
		t.Fatalf("expected a non-determinism error, got %T: %v", err, err)
	}
}
