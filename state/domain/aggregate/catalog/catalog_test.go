package catalog_test

import (
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

func loadedCatalog(initial map[string]catalog.Entry) *catalog.ScheduleCatalog {
	return catalog.Hydrate(initial, 0)
}

func TestReconcile_RejectsEmptyList(t *testing.T) {
	c := loadedCatalog(map[string]catalog.Entry{"x": {ScheduleName: "x"}})
	err := c.Reconcile(1, nil, nil, time.Now())
	if err != catalog.ErrEmptyReconciliation {
		t.Fatalf("err: got %v want ErrEmptyReconciliation", err)
	}
}

func TestReconcile_AddsNewName(t *testing.T) {
	c := loadedCatalog(nil)
	meta := map[string]map[string]run.ServiceMetadata{
		"orders": {"users": {ImageTag: "abc"}},
	}
	err := c.Reconcile(1, []string{"orders"}, meta, time.Now())
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
	err := c.Reconcile(1, []string{"orders"}, nil, time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC))
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
	err := c.Reconcile(1, []string{"orders"}, nil, time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, _ := c.Entry("orders"); !e.IsActive() {
		t.Fatalf("expected 'orders' reactivated")
	}
}

func TestReconcile_RefusesAnOlderPromotionAndChangesNothing(t *testing.T) {
	c := catalog.Hydrate(map[string]catalog.Entry{
		"orders": {ScheduleName: "orders"},
		"users":  {ScheduleName: "users"},
	}, 5)
	err := c.Reconcile(4, []string{"orders"}, nil, time.Now())
	if !errors.Is(err, catalog.ErrStalePromotion) {
		t.Fatalf("err: got %v want ErrStalePromotion", err)
	}
	if e, _ := c.Entry("users"); !e.IsActive() {
		t.Fatalf("an older promotion must not soft-delete 'users'")
	}
	if got := c.PromotionSeq(); got != 5 {
		t.Fatalf("PromotionSeq: got %d want 5", got)
	}
}

func TestReconcile_StaleCheckPrecedesTheEmptyListGuard(t *testing.T) {
	// An older payload is acknowledged as stale even when its list is empty:
	// it describes a release the catalog has already moved past.
	c := catalog.Hydrate(map[string]catalog.Entry{"orders": {ScheduleName: "orders"}}, 5)
	if err := c.Reconcile(4, nil, nil, time.Now()); !errors.Is(err, catalog.ErrStalePromotion) {
		t.Fatalf("err: got %v want ErrStalePromotion", err)
	}
}

func TestReconcile_ReappliesTheSamePromotion(t *testing.T) {
	c := catalog.Hydrate(map[string]catalog.Entry{
		"orders": {ScheduleName: "orders"},
		"users":  {ScheduleName: "users"},
	}, 5)
	if err := c.Reconcile(5, []string{"orders"}, nil, time.Now()); err != nil {
		t.Fatalf("err: %v", err)
	}
	if e, _ := c.Entry("users"); e.IsActive() {
		t.Fatalf("an equal promotion seq re-applies: expected 'users' soft-deleted")
	}
	if got := c.PromotionSeq(); got != 5 {
		t.Fatalf("PromotionSeq: got %d want 5", got)
	}
}

func TestReconcile_RecordsANewerPromotion(t *testing.T) {
	c := catalog.Hydrate(nil, 5)
	if err := c.Reconcile(9, []string{"orders"}, nil, time.Now()); err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := c.PromotionSeq(); got != 9 {
		t.Fatalf("PromotionSeq: got %d want 9", got)
	}
}
