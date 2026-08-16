package frontend

import (
	"context"
	"testing"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAPIKeyUnaryInterceptor(t *testing.T) {
	// "admin-key" is unscoped (any namespace); "ops-key" is scoped to
	// default/billing; "ro-key" is scoped to a single namespace via the
	// explicit "*" spelling of unrestricted, to prove that's equivalent to
	// omitting the scope entirely.
	interceptor := NewAPIKeyUnaryInterceptor([]string{
		"admin-key",
		"ops-key:default|billing",
		"star-key:*",
	})
	var handlerCalled bool
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	tests := []struct {
		name    string
		md      metadata.MD
		req     any
		wantErr codes.Code
	}{
		{"no metadata at all", nil, nil, codes.Unauthenticated},
		{"missing key header", metadata.Pairs("other", "x"), nil, codes.Unauthenticated},
		{"empty key value", metadata.Pairs(apiKeyMetadataKey, ""), nil, codes.Unauthenticated},
		{"wrong key", metadata.Pairs(apiKeyMetadataKey, "not-a-real-key"), nil, codes.Unauthenticated},
		{"unscoped key, no namespace field on request", metadata.Pairs(apiKeyMetadataKey, "admin-key"), &flowv1.RespondActivityTaskCompletedRequest{}, codes.OK},
		{"unscoped key, any namespace", metadata.Pairs(apiKeyMetadataKey, "admin-key"), &flowv1.DescribeWorkflowExecutionRequest{Namespace: "billing"}, codes.OK},
		{"star-scoped key behaves as unrestricted", metadata.Pairs(apiKeyMetadataKey, "star-key"), &flowv1.DescribeWorkflowExecutionRequest{Namespace: "anything"}, codes.OK},
		{"scoped key, allowed namespace", metadata.Pairs(apiKeyMetadataKey, "ops-key"), &flowv1.DescribeWorkflowExecutionRequest{Namespace: "billing"}, codes.OK},
		{"scoped key, default namespace via explicit empty field", metadata.Pairs(apiKeyMetadataKey, "ops-key"), &flowv1.StartWorkflowExecutionRequest{Namespace: ""}, codes.OK},
		{"scoped key, disallowed namespace", metadata.Pairs(apiKeyMetadataKey, "ops-key"), &flowv1.DescribeWorkflowExecutionRequest{Namespace: "payments"}, codes.PermissionDenied},
		{"scoped key, request without a namespace field bypasses the check", metadata.Pairs(apiKeyMetadataKey, "ops-key"), &flowv1.RespondWorkflowTaskCompletedRequest{}, codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerCalled = false
			ctx := context.Background()
			if tt.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.md)
			}
			_, err := interceptor(ctx, tt.req, &grpc.UnaryServerInfo{}, handler)
			if tt.wantErr == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !handlerCalled {
					t.Fatal("handler was not called on valid key/scope")
				}
				return
			}
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if got := status.Code(err); got != tt.wantErr {
				t.Fatalf("got code %v, want %v", got, tt.wantErr)
			}
			if handlerCalled {
				t.Fatal("handler was called despite rejected auth")
			}
		})
	}
}

func TestParseAPIKeysNamespaceScoping(t *testing.T) {
	table := parseAPIKeys([]string{
		"bare-key",
		" spaced-key : default | billing ",
		"star-key:*",
		"empty-scope-key:",
		":no-key-name",
	})

	if scope, ok := table["bare-key"]; !ok || scope.namespaces != nil {
		t.Fatalf("bare-key: got %+v, want unrestricted", scope)
	}
	if scope, ok := table["star-key"]; !ok || scope.namespaces != nil {
		t.Fatalf("star-key: got %+v, want unrestricted (explicit *)", scope)
	}
	if _, ok := table["no-key-name"]; ok {
		t.Fatal("an entry with no key name before ':' should be dropped, not registered")
	}

	scope, ok := table["spaced-key"]
	if !ok {
		t.Fatal("spaced-key: not found — whitespace around ':' and '|' should be trimmed")
	}
	if !scope.allows("default") || !scope.allows("billing") {
		t.Fatalf("spaced-key: got namespaces %v, want default+billing", scope.namespaces)
	}
	if scope.allows("payments") {
		t.Fatal("spaced-key: should not allow an unlisted namespace")
	}

	if scope, ok := table["empty-scope-key"]; !ok || scope.namespaces == nil || len(scope.namespaces) != 0 {
		t.Fatalf("empty-scope-key: got %+v, want a non-nil but empty scope (allows nothing)", scope)
	} else if scope.allows("default") {
		t.Fatal("empty-scope-key: a ':' with nothing after it should allow no namespace, not all")
	}
}
