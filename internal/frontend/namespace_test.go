package frontend

import (
	"context"
	"testing"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCreateNamespaceRejectsEmptyName proves this is checked before the
// store is ever touched: s here has a nil *history.Store, so a nil-pointer
// panic would occur immediately if CreateNamespace tried to use it.
func TestCreateNamespaceRejectsEmptyName(t *testing.T) {
	s := newTestServer()
	_, err := s.CreateNamespace(context.Background(), &flowv1.CreateNamespaceRequest{Name: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}
