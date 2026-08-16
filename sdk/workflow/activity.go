package workflow

import (
	"github.com/krishnakichuu/flowd/sdk/internal/converter"
	"github.com/krishnakichuu/flowd/sdk/internal/funcname"
	"github.com/krishnakichuu/flowd/sdk/internal/replayer"
)

// ExecuteActivity schedules activityFn — a reference to a registered
// activity function, following the func(activity.Context, In) (Out, error)
// convention — with a single input argument. activityFn is never invoked
// locally; reflection is used only to derive its registered name (see
// worker.RegisterActivity), matching how Worker.RegisterActivity derives
// the same name so the two agree without the caller hardcoding a string.
func ExecuteActivity(ctx Context, activityFn any, arg any, opts ActivityOptions) Future {
	name := funcname.Of(activityFn)
	payload, err := converter.Default.ToPayload(arg)
	if err != nil {
		// A marshal failure here is a programming error (an unmarshalable
		// argument type), not a runtime condition workflow code can
		// meaningfully recover from.
		panic(err)
	}
	f := ctx.exec.ScheduleActivity(name, payload, replayer.ActivityOptions{
		RetryPolicy:            opts.RetryPolicy,
		ScheduleToStartTimeout: opts.ScheduleToStartTimeout,
		StartToCloseTimeout:    opts.StartToCloseTimeout,
	})
	return Future{co: ctx.co, f: f}
}
