// Package activity provides the context type passed to activity functions.
// Unlike workflow.Context, activities have no determinism constraint (only
// their recorded result is replayed, never their code — see ADR-0001), so
// activity.Context wraps a real context.Context and activities may perform
// arbitrary I/O.
package activity

import "context"

type Info struct {
	ActivityID   int64
	ActivityType string
	Attempt      int32
}

type Context struct {
	context.Context
	info Info
}

func NewContext(std context.Context, info Info) Context {
	return Context{Context: std, info: info}
}

func GetInfo(ctx Context) Info {
	return ctx.info
}
