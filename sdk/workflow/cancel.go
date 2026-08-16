package workflow

// IsCancelRequested reports whether a client has called
// RequestCancelWorkflowExecution against this run. This is cooperative,
// not automatic: nothing in the SDK stops workflow code on its own — a
// workflow that never checks this runs to completion exactly as if
// cancellation had never been requested. A workflow that wants to react
// checks this at its own natural decision points (e.g. the top of a loop,
// or right after an activity returns), runs whatever compensation it
// needs (also via ExecuteActivity — cleanup work has the same
// determinism rules as any other activity call), then returns however it
// sees fit: a normal result, or an error the caller will see as a failed
// run. Once true, it stays true for the rest of this run.
func IsCancelRequested(ctx Context) bool {
	return ctx.exec.IsCancelRequested()
}

// CancelReason returns the reason string passed to
// RequestCancelWorkflowExecution, or "" if IsCancelRequested is false.
func CancelReason(ctx Context) string {
	return ctx.exec.CancelReason()
}
