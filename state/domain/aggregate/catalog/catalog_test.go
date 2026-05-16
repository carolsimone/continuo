package catalog_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

func loadedCatalog(initial map[string]catalog.Entry) *catalog.ScheduleCatalog {
	return catalog.Hydrate(initial)
}

func TestReconcile_RejectsEmptyList(t *testing.T) {
	c := loadedCatalog(map[string]catalog.Entry{"x": {ScheduleName: "x"}})
	_, err := c.Reconcile(nil, nil, time.Now())
	if err != catalog.ErrEmptyReconciliation {
		t.Fatalf("err: got %v want ErrEmptyReconciliation", err)
	}
}

func TestReconcile_AddsNewName(t *testing.T) {
	c := loadedCatalog(nil)
	meta := map[string]map[string]run.ServiceMetadata{
		"orders": {"users": {ManifestVersion: "v1", ImageTag: "abc"}},
	}
	events, err := c.Reconcile([]string{"orders"}, meta, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, ok := c.Entry("orders"); !ok || !e.IsActive() {
		t.Fatalf("expected active entry 'orders'")
	}
	if got := events[0].(catalog.CatalogReconciled); len(got.Added) != 1 || got.Added[0] != "orders" {
		t.Fatalf("expected Added=[orders], got %+v", got)
	}
}

func TestReconcile_SoftDeletesAbsentName(t *testing.T) {
	c := loadedCatalog(map[string]catalog.Entry{
		"orders": {ScheduleName: "orders"},
		"users":  {ScheduleName: "users"},
	})
	events, err := c.Reconcile([]string{"orders"}, nil, time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, _ := c.Entry("users"); e.IsActive() {
		t.Fatalf("expected 'users' soft-deleted")
	}
	rec := events[0].(catalog.CatalogReconciled)
	if len(rec.Removed) != 1 || rec.Removed[0] != "users" {
		t.Fatalf("expected Removed=[users], got %+v", rec)
	}
}

func TestReconcile_ReactivatesRemovedName(t *testing.T) {
	removedAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	c := loadedCatalog(map[string]catalog.Entry{
		"orders": {ScheduleName: "orders", RemovedAt: &removedAt},
	})
	events, err := c.Reconcile([]string{"orders"}, nil, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, _ := c.Entry("orders"); !e.IsActive() {
		t.Fatalf("expected 'orders' reactivated")
	}
	rec := events[0].(catalog.CatalogReconciled)
	if len(rec.Reactivated) != 1 || rec.Reactivated[0] != "orders" {
		t.Fatalf("expected Reactivated=[orders], got %+v", rec)
	}
}
