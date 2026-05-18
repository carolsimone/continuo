package catalog

// DomainEvent is the marker interface for catalog-emitted events.
type DomainEvent interface{ catalogDomainEvent() }

// CatalogReconciled is recorded after a successful Reconcile. There is no
// downstream consumer today; the event is buffered for symmetry with the
// Run aggregate and for future use.
type CatalogReconciled struct {
	Added       []string
	Removed     []string
	Reactivated []string
}

func (CatalogReconciled) catalogDomainEvent() {}
