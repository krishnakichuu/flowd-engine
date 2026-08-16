package history

import (
	"fmt"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewEvent is one history event to append, prior to event_id assignment by
// AppendHistory.
type NewEvent struct {
	EventType  flowv1.HistoryEventType
	Attributes proto.Message
}

// decodeHistoryEvent reconstructs a typed flowv1.HistoryEvent from a
// persisted row: attributes are stored as marshaled protobuf bytes of
// exactly the message type that matches event_type (ADR-0002), so decoding
// is a lookup-then-unmarshal keyed on event_type.
func decodeHistoryEvent(row sqlc.HistoryEvent) (*flowv1.HistoryEvent, error) {
	evType := flowv1.HistoryEventType(flowv1.HistoryEventType_value[row.EventType])
	ev := &flowv1.HistoryEvent{
		EventId:   row.EventID,
		EventTime: timestamppb.New(row.EventTime.Time),
		EventType: evType,
	}

	var attrs proto.Message
	switch evType {
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		m := &flowv1.WorkflowExecutionStartedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		m := &flowv1.WorkflowExecutionCompletedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		m := &flowv1.WorkflowExecutionFailedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		m := &flowv1.WorkflowExecutionTerminatedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		m := &flowv1.WorkflowExecutionSignaledEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
		m := &flowv1.WorkflowExecutionCancelRequestedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
		m := &flowv1.WorkflowTaskScheduledEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_STARTED:
		m := &flowv1.WorkflowTaskStartedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
		m := &flowv1.WorkflowTaskCompletedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_TASK_FAILED:
		m := &flowv1.WorkflowTaskFailedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		m := &flowv1.ActivityTaskScheduledEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_STARTED:
		m := &flowv1.ActivityTaskStartedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		m := &flowv1.ActivityTaskCompletedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_ACTIVITY_TASK_FAILED:
		m := &flowv1.ActivityTaskFailedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_TIMER_STARTED:
		m := &flowv1.TimerStartedEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_TIMER_FIRED:
		m := &flowv1.TimerFiredEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: m}
	case flowv1.HistoryEventType_HISTORY_EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		m := &flowv1.WorkflowExecutionContinuedAsNewEventAttributes{}
		attrs = m
		ev.Attributes = &flowv1.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: m}
	default:
		return nil, fmt.Errorf("history: unknown event_type %q for event %d", row.EventType, row.EventID)
	}

	if err := proto.Unmarshal(row.Attributes, attrs); err != nil {
		return nil, fmt.Errorf("history: unmarshal attributes for event %d (%s): %w", row.EventID, row.EventType, err)
	}
	return ev, nil
}
