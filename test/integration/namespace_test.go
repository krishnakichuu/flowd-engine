//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/krishnakichuu/flowd/internal/history"
	postgrespkg "github.com/krishnakichuu/flowd/internal/persistence/postgres"
)

// TestCreateAndListNamespaces exercises namespace management (Phase 2
// roadmap, Track D, item 4) directly against real Postgres: creating a
// namespace makes it show up in ListNamespaces, and creating the same name
// twice is rejected as a conflict rather than silently duplicating or
// erroring some other way.
func TestCreateAndListNamespaces(t *testing.T) {
	ctx := context.Background()
	pool, err := postgrespkg.NewPool(ctx, databaseDSN())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	store := history.NewStore(pool, history.StoreOptions{})
	name := fmt.Sprintf("ns-test-%d", time.Now().UnixNano())

	before, err := store.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("list namespaces before create: %v", err)
	}
	for _, ns := range before {
		if ns.Name == name {
			t.Fatalf("namespace %q already exists before this test created it", name)
		}
	}

	created, err := store.CreateNamespace(ctx, name)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if created.Name != name {
		t.Fatalf("created namespace name = %q, want %q", created.Name, name)
	}
	if created.CreatedAt.IsZero() {
		t.Fatal("created namespace has a zero CreatedAt")
	}

	after, err := store.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("list namespaces after create: %v", err)
	}
	found := false
	for _, ns := range after {
		if ns.Name == name {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("namespace %q not present in ListNamespaces after CreateNamespace", name)
	}

	_, err = store.CreateNamespace(ctx, name)
	if !errors.Is(err, history.ErrNamespaceAlreadyExists) {
		t.Fatalf("creating a duplicate namespace: got %v, want ErrNamespaceAlreadyExists", err)
	}
}
