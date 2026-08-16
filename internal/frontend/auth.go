package frontend

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// apiKeyMetadataKey is the gRPC metadata key clients attach their API key
// under (sdk/client sends it on every outgoing call when Options.APIKey is
// set). gRPC lower-cases metadata keys, so this must already be lowercase.
const apiKeyMetadataKey = "x-flowd-api-key" //nolint:gosec // this is a metadata header name, not a credential value

// apiKeyScope is what a single API key is authorized to do. A nil
// namespaces set means unrestricted — the key may act in any namespace,
// either because its FLOWD_API_KEYS entry had no ":ns1|ns2" suffix at all,
// or the suffix was explicitly "*".
type apiKeyScope struct {
	namespaces map[string]struct{}
}

func (s apiKeyScope) allows(namespace string) bool {
	if s.namespaces == nil {
		return true
	}
	_, ok := s.namespaces[namespace]
	return ok
}

// parseAPIKeys turns FLOWD_API_KEYS entries into a lookup table. Each entry
// is either a bare key ("admin-key", unrestricted — any namespace) or a
// scoped one ("ops-key:default|billing", restricted to the listed
// namespaces). This is parsed here rather than in internal/config because
// the scoping syntax is auth policy, not generic env-var loading.
func parseAPIKeys(keys []string) map[string]apiKeyScope {
	table := make(map[string]apiKeyScope, len(keys))
	for _, entry := range keys {
		key, nsList, hasScope := strings.Cut(entry, ":")
		if key = strings.TrimSpace(key); key == "" {
			continue
		}
		if !hasScope {
			table[key] = apiKeyScope{}
			continue
		}
		namespaces := make(map[string]struct{})
		unrestricted := false
		for ns := range strings.SplitSeq(nsList, "|") {
			if ns = strings.TrimSpace(ns); ns == "" {
				continue
			}
			if ns == "*" {
				unrestricted = true
				break
			}
			namespaces[ns] = struct{}{}
		}
		if unrestricted {
			table[key] = apiKeyScope{}
		} else {
			table[key] = apiKeyScope{namespaces: namespaces}
		}
	}
	return table
}

// namespacedRequest is implemented by every WorkflowService request message
// that carries an explicit namespace field — protoc-gen-go generates
// GetNamespace() for any proto field named "namespace". 7 of the service's
// 10 RPCs have one (Start/Signal/GetHistory/Describe/Terminate plus both
// Poll RPCs). The remaining 4 — RespondWorkflowTaskCompleted/Failed and
// RespondActivityTaskCompleted/Failed — only carry an opaque task token, no
// namespace field to check: a token is only obtainable by first
// successfully polling within an already-authorized namespace (Poll* is
// namespace-checked below), so there's no separate namespace to enforce at
// respond time — the poll that issued the token already did.
type namespacedRequest interface {
	GetNamespace() string
}

// NewAPIKeyUnaryInterceptor returns a UnaryServerInterceptor that rejects
// any RPC not presenting one of keys via the x-flowd-api-key metadata
// header, and — for RPCs whose request names a namespace — rejects calls
// outside that key's configured scope with PermissionDenied. Phase 1 had
// neither check: any client reachable on the network was implicitly
// trusted to act in any namespace (see Phase 2 roadmap, Track A, items 2
// and 3). Call only when len(keys) > 0 — cmd/flowd installs it
// conditionally so a deployment that hasn't configured FLOWD_API_KEYS keeps
// Phase 1's original (trusted-network) behavior unchanged.
func NewAPIKeyUnaryInterceptor(keys []string) grpc.UnaryServerInterceptor {
	table := parseAPIKeys(keys)
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}
		vals := md.Get(apiKeyMetadataKey)
		if len(vals) == 0 || vals[0] == "" {
			return nil, status.Error(codes.Unauthenticated, "missing api key")
		}
		scope, ok := table[vals[0]]
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid api key")
		}
		if nsReq, ok := req.(namespacedRequest); ok {
			ns := namespaceOrDefault(nsReq.GetNamespace())
			if !scope.allows(ns) {
				return nil, status.Errorf(codes.PermissionDenied, "api key not authorized for namespace %q", ns)
			}
		}
		return handler(ctx, req)
	}
}
