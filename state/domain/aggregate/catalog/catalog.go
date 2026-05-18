package catalog

import (
	"sort"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// ScheduleCatalog is the aggregate root for the schedule_catalog table.
// Loaded as a whole-set view via CatalogRepository.LoadCatalogForUpdate and
// persisted via SaveCatalog after Reconcile.
type ScheduleCatalog struct {
	entries map[string]Entry
	changes catalogChanges
	events  []DomainEvent
}

type catalogChanges struct {
	added       []string
	removed     []string
	reactivated []string
	metadata    map[string]map[string]run.ServiceMetadata // name → service → meta
}

// Hydrate constructs a ScheduleCatalog from persisted rows. Used by the
// postgres adapter inside LoadCatalogForUpdate / GetCatalog.
func Hydrate(initial map[string]Entry) *ScheduleCatalog {
	if initial == nil {
		initial = map[string]Entry{}
	}
	return &ScheduleCatalog{entries: initial}
}

// Entry returns the catalog entry for `name` plus a presence flag.
func (c *ScheduleCatalog) Entry(name string) (Entry, bool) {
	e, ok := c.entries[name]
	return e, ok
}

// Names returns the names of every entry, sorted.
func (c *ScheduleCatalog) Names() []string {
	out := make([]string, 0, len(c.entries))
	for k := range c.entries {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Reconcile applies a schedules.loaded:v1 payload. See package errors for the
// empty-list guard.
func (c *ScheduleCatalog) Reconcile(
	presentNames []string,
	serviceMetadata map[string]map[string]run.ServiceMetadata,
	now time.Time,
) ([]DomainEvent, error) {
	if len(presentNames) == 0 {
		return nil, ErrEmptyReconciliation
	}
	c.changes = catalogChanges{metadata: map[string]map[string]run.ServiceMetadata{}}
	present := map[string]bool{}
	for _, name := range presentNames {
		present[name] = true
		existing, ok := c.entries[name]
		if !ok {
			c.entries[name] = Entry{ScheduleName: name, ServiceMetadata: cloneSvcMeta(serviceMetadata[name])}
			c.changes.added = append(c.changes.added, name)
		} else if !existing.IsActive() {
			existing.RemovedAt = nil
			existing.ServiceMetadata = cloneSvcMeta(serviceMetadata[name])
			c.entries[name] = existing
			c.changes.reactivated = append(c.changes.reactivated, name)
		} else {
			existing.ServiceMetadata = cloneSvcMeta(serviceMetadata[name])
			c.entries[name] = existing
		}
		c.changes.metadata[name] = cloneSvcMeta(serviceMetadata[name])
	}
	for name, entry := range c.entries {
		if present[name] || !entry.IsActive() {
			continue
		}
		removed := now
		entry.RemovedAt = &removed
		c.entries[name] = entry
		c.changes.removed = append(c.changes.removed, name)
	}
	sort.Strings(c.changes.added)
	sort.Strings(c.changes.removed)
	sort.Strings(c.changes.reactivated)

	evt := CatalogReconciled{
		Added:       c.changes.added,
		Removed:     c.changes.removed,
		Reactivated: c.changes.reactivated,
	}
	c.events = append(c.events, evt)
	return []DomainEvent{evt}, nil
}

// Changes returns the changeset for the repository adapter.
func (c *ScheduleCatalog) Changes() catalogChanges { return c.changes }
func (c *ScheduleCatalog) ResetChanges()           { c.changes = catalogChanges{} }

// PullEvents matches the Run aggregate's API.
func (c *ScheduleCatalog) PullEvents() []DomainEvent {
	out := c.events
	c.events = nil
	return out
}

func cloneSvcMeta(m map[string]run.ServiceMetadata) map[string]run.ServiceMetadata {
	if m == nil {
		return nil
	}
	out := make(map[string]run.ServiceMetadata, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
