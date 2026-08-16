package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// NamespaceInfo is one row from the namespaces table, in the shape callers
// outside this package need — internal/frontend maps it directly onto
// flowv1.NamespaceInfo.
type NamespaceInfo struct {
	Name      string
	CreatedAt time.Time
}

// CreateNamespace inserts a new namespace. namespaces.name is UNIQUE (see
// migration 0001), so a duplicate name is detected via the
// ON CONFLICT ... DO NOTHING RETURNING pattern: zero rows back means the
// name was already taken, not a genuine error.
func (s *Store) CreateNamespace(ctx context.Context, name string) (NamespaceInfo, error) {
	q := sqlc.New(s.pool)
	ns, err := q.CreateNamespace(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return NamespaceInfo{}, ErrNamespaceAlreadyExists
		}
		return NamespaceInfo{}, fmt.Errorf("history: create namespace: %w", err)
	}
	return NamespaceInfo{Name: ns.Name, CreatedAt: ns.CreatedAt.Time}, nil
}

// ListNamespaces returns every namespace, ordered by name.
func (s *Store) ListNamespaces(ctx context.Context) ([]NamespaceInfo, error) {
	q := sqlc.New(s.pool)
	rows, err := q.ListNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: list namespaces: %w", err)
	}
	out := make([]NamespaceInfo, len(rows))
	for i, r := range rows {
		out[i] = NamespaceInfo{Name: r.Name, CreatedAt: r.CreatedAt.Time}
	}
	return out, nil
}
