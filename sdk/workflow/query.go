package workflow

import (
	"fmt"
	"reflect"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
)

// SetQueryHandler registers handler — a func(In) (Out, error) — to answer
// synchronous QueryWorkflowExecution calls of queryType against this
// workflow's current state, without waiting for completion or guessing
// from DescribeWorkflowExecution's status alone (Phase 2 roadmap, Track D,
// item 1). Unlike ExecuteActivity/Sleep, a query handler is not a
// blocking primitive: it must read already-captured local state and
// return immediately — never call ExecuteActivity, Sleep, or anything
// else that yields from inside one.
//
// Call this once, near the top of the workflow function, before its first
// blocking call: the very first ExecuteRound already runs the workflow up
// to that first blocking point, which is what makes the handler
// registered and answerable — via a cache hit or a fresh replay alike —
// well before any query could actually arrive.
func SetQueryHandler(ctx Context, queryType string, handler any) error {
	v := reflect.ValueOf(handler)
	t := v.Type()
	if t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 2 {
		return fmt.Errorf("workflow: query handler must be func(In) (Out, error), got %s", t)
	}
	if !t.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("workflow: query handler's second return value must be error, got %s", t)
	}

	ctx.exec.SetQueryHandler(queryType, func(args *flowv1.Payload) (*flowv1.Payload, error) {
		inPtr := reflect.New(t.In(0))
		if err := converter.Default.FromPayload(args, inPtr.Interface()); err != nil {
			return nil, fmt.Errorf("query %q: unmarshal args: %w", queryType, err)
		}
		out := v.Call([]reflect.Value{inPtr.Elem()})
		if errVal := out[1]; !errVal.IsNil() {
			return nil, errVal.Interface().(error)
		}
		return converter.Default.ToPayload(out[0].Interface())
	})
	return nil
}
