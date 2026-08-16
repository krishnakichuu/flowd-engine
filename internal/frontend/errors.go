package frontend

import (
	"errors"

	"github.com/krishnakichuu/flowd/internal/history"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toGRPCError maps internal/history's sentinel errors to the gRPC status
// codes a well-behaved client/SDK can act on distinctly (e.g. retry a
// NotFound differently than an AlreadyExists conflict).
func toGRPCError(err error) error {
	switch {
	case errors.Is(err, history.ErrWorkflowAlreadyRunning), errors.Is(err, history.ErrNamespaceAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, history.ErrWorkflowNotFound), errors.Is(err, history.ErrNamespaceNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, history.ErrTaskLeaseExpired):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, history.ErrConcurrentModification):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, history.ErrNoTaskAvailable):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
