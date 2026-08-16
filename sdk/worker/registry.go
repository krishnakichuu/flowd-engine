package worker

import (
	"fmt"
	"reflect"

	"github.com/krishnakichuu/flowd/sdk/internal/funcname"
)

type workflowFunc struct {
	fn        reflect.Value
	inputType reflect.Type
}

type activityFunc struct {
	fn        reflect.Value
	inputType reflect.Type
}

// RegisterWorkflow registers fn, which must follow the
// func(workflow.Context, In) (Out, error) convention. Its registered name
// (used as workflow_type on the wire) is derived by reflection from fn's
// own name, matching client.StartWorkflow's derivation so the two agree
// without a hardcoded string.
func (w *Worker) RegisterWorkflow(fn any) {
	v := reflect.ValueOf(fn)
	t := v.Type()
	mustBeSDKFunc(t)
	w.workflows[funcname.Of(fn)] = workflowFunc{fn: v, inputType: t.In(1)}
}

// RegisterActivity registers fn, which must follow the
// func(activity.Context, In) (Out, error) convention.
func (w *Worker) RegisterActivity(fn any) {
	v := reflect.ValueOf(fn)
	t := v.Type()
	mustBeSDKFunc(t)
	w.activities[funcname.Of(fn)] = activityFunc{fn: v, inputType: t.In(1)}
}

// mustBeSDKFunc validates fn matches the SDK's func(Context, In) (Out,
// error) shape at registration time, so a mismatched signature fails loudly
// on startup rather than as an obscure reflect panic mid-task.
func mustBeSDKFunc(t reflect.Type) {
	if t.Kind() != reflect.Func || t.NumIn() != 2 || t.NumOut() != 2 {
		panic(fmt.Sprintf("worker: registered function must be func(Context, In) (Out, error), got %s", t))
	}
	if !t.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic(fmt.Sprintf("worker: registered function's second return value must be error, got %s", t))
	}
}
