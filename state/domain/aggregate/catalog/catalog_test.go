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
	err := c.Reconcile(nil, nil, time.Now())
	if err != catalog.ErrEmptyReconciliation {
		t.Fatalf("err: got %v want ErrEmptyReconciliation", err)
	}
}

func TestReconcile_AddsNewName(t *testing.T) {
	c := loadedCatalog(nil)
	meta := map[string]map[string]run.ServiceMetadata{
		"orders": {"users": {ManifestVersion: "v1", ImageTag: "abc"}},
	}
	err := c.Reconcile([]string{"orders"}, meta, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, ok := c.Entry("orders"); !ok || !e.IsActive() {
		t.Fatalf("expected active entry 'orders'")
	}
}

func TestReconcile_SoftDeletesAbsentName(t *testing.T) {
	c := loadedCatalog(map[string]catalog.Entry{
		"orders": {ScheduleName: "orders"},
		"users":  {ScheduleName: "users"},
	})
	err := c.Reconcile([]string{"orders"}, nil, time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, _ := c.Entry("users"); e.IsActive() {
		t.Fatalf("expected 'users' soft-deleted")
	}
}

func TestReconcile_ReactivatesRemovedName(t *testing.T) {
	removedAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	c := loadedCatalog(map[string]catalog.Entry{
		"orders": {ScheduleName: "orders", RemovedAt: &removedAt},
	})
	err := c.Reconcile([]string{"orders"}, nil, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, _ := c.Entry("orders"); !e.IsActive() {
		t.Fatalf("expected 'orders' reactivated")
	}
}
