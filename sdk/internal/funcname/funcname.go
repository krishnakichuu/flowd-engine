// Package funcname derives a stable registration name from a Go function
// reference. It is shared by worker.RegisterWorkflow/RegisterActivity,
// workflow.ExecuteActivity, and client.StartWorkflow so all three agree on
// the same name for the same function without any of them hardcoding a
// string — getting this wrong silently breaks dispatch (a worker registers
// under one name, a caller sends another, and the task is never claimed).
package funcname

import (
	"reflect"
	"runtime"
	"strings"
)

// Of returns fn's unqualified function name, e.g. "GreetActivity" for a
// package-level func GreetActivity(...).
func Of(fn any) string {
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	if idx := strings.LastIndex(full, "."); idx >= 0 {
		return full[idx+1:]
	}
	return full
}
