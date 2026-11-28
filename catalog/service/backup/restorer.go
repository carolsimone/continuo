package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"

	"github.com/carolsimone/continuo/catalog/service/graphbackup"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

// ErrPartialRestore means a restore wiped the tenant's graph and did not
// finish loading the backup.
var ErrPartialRestore = errors.New("the tenant's graph is partial and its schema migration has not run; " +
	"run the restore again: it wipes the tenant's graph first, so a second run repairs it")

// RestoreResult is what a restore loaded: the node and relationship records of
// the backup.
type RestoreResult struct {
	Nodes         int64
	Relationships int64
}

// Restorer returns a tenant's graph to one of its backups.
type Restorer struct {
	keeper leaseKeeper
	graph  ports.GraphLoader
	store  ports.BackupStore
	logger *slog.Logger
}

// NewRestorer returns a Restorer that loads backups from store into graph.
func NewRestorer(lease ports.BackupLease, graph ports.GraphLoader, store ports.BackupStore, logger *slog.Logger) *Restorer {
	return &Restorer{keeper: newLeaseKeeper(lease, logger), graph: graph, store: store, logger: logger}
}

// Restore returns the graph of the tenant key belongs to (the store locates
// it) to the backup stored at key.
//
// It reads the backup twice and never holds it whole. The first read checks
// the whole backup (BackupStore.Validate) with no lease and no write, so a
// backup that cannot be loaded exactly, or that names another tenant, leaves
// the graph untouched. Then it takes the tenant's backup lease (ErrSkipped
// while another backup or restore holds it), opens the backup again and
// checks its header against the first read's before it wipes the tenant's
// graph, loads the backup, confirms the lease is still held and releases it.
// While it holds the lease, a step that moves no record for stallLimit (a
// database or store that never answers) stops it with ErrStalled and
// releases the lease. A failure after the wipe returns ErrPartialRestore.
func (r *Restorer) Restore(ctx context.Context, key string) (RestoreResult, error) {
	tenantID, _, err := r.store.Locate(key)
	if err != nil {
		return RestoreResult{}, err
	}
	header, err := r.store.Validate(ctx, key, tenantID)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("the backup %s cannot be restored, and the graph of tenant %q is untouched: %w", key, tenantID, err)
	}
	holder := newHolder(ports.LeaseRestore)
	if err := r.keeper.acquire(ctx, tenantID, ports.LeaseRestore, holder); err != nil {
		return RestoreResult{}, fmt.Errorf("restore of tenant %q: %w", tenantID, err)
	}
	res, err := r.load(ctx, tenantID, holder, key, header)
	if releaseErr := r.keeper.release(ctx, tenantID, holder); releaseErr != nil {
		r.logger.Error("restore lease not released: the tenant's writes resume when it expires",
			"tenant_id", tenantID, "expires_within", r.keeper.ttl.String(), "error", releaseErr)
	}
	if err != nil {
		r.logger.Error("catalog restore failed", "tenant_id", tenantID, "object_key", key, "error", err)
		return RestoreResult{}, err
	}
	r.logger.Info("catalog restore loaded", "tenant_id", tenantID, "object_key", key,
		"nodes", res.Nodes, "relationships", res.Relationships)
	return res, nil
}

// load opens the backup again, checks it is the one Validate read, wipes the
// tenant's graph and restores the backup, while holder's lease is held,
// renewed and watched. Opening the backup and every record read is progress,
// and so is the start of the wipe and of the restore, which move no record
// while the database works.
func (r *Restorer) load(ctx context.Context, tenantID, holder, key string, validated graphbackup.Header) (RestoreResult, error) {
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := &progress{}
	stop := r.keeper.watch(workCtx, cancel, tenantID, holder, work)
	defer stop()
	in, err := r.store.OpenRead(workCtx, key)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("the backup %s cannot be read again, and the graph of tenant %q is untouched: %w", key, tenantID, err)
	}
	defer func() { _ = in.Close() }()
	work.advance()
	if !sameHeader(in.Header(), validated) {
		return RestoreResult{}, fmt.Errorf("the backup %s changed since it was validated, and the graph of tenant %q is untouched: "+
			"its header differs from the one validated", key, tenantID)
	}

	partial := func(step string, err error) error {
		if cause := stopCause(workCtx); cause != nil {
			err = errors.Join(err, cause)
		}
		return fmt.Errorf("restore of tenant %q from %s: %s: %w: %w", tenantID, key, step, ErrPartialRestore, err)
	}
	work.advance()
	if err := r.graph.Wipe(workCtx, tenantID); err != nil {
		return RestoreResult{}, partial("wipe the graph", err)
	}
	work.advance()
	counted := &countingReader{RecordReader: in, work: work}
	if err := r.graph.Load(workCtx, tenantID, counted); err != nil {
		return RestoreResult{}, partial("load the backup", err)
	}
	if cause := stopCause(workCtx); cause != nil {
		return RestoreResult{}, partial("load the backup", cause)
	}
	if err := r.keeper.confirm(workCtx, tenantID, holder); err != nil {
		return RestoreResult{}, fmt.Errorf("restore of tenant %q from %s: the lease may have lapsed while the backup loaded, "+
			"so writes may have landed in the graph during the restore; run the restore again: %w", tenantID, key, err)
	}
	return RestoreResult{Nodes: counted.nodes, Relationships: counted.rels}, nil
}

// sameHeader reports whether two backup headers are equal.
func sameHeader(a, b graphbackup.Header) bool {
	return a.FormatVersion == b.FormatVersion && a.TenantID == b.TenantID && a.TakenAt.Equal(b.TakenAt) &&
		a.CatalogVersion == b.CatalogVersion && maps.Equal(a.Nodes, b.Nodes) && maps.Equal(a.Relationships, b.Relationships)
}

// countingReader counts the node and relationship records read through it,
// each record being progress; a backup lease node in the backup, which a
// restore does not load, is not counted.
type countingReader struct {
	ports.RecordReader
	work        *progress
	nodes, rels int64
}

func (c *countingReader) Next() (any, error) {
	rec, err := c.RecordReader.Next()
	if err != nil {
		return rec, err
	}
	c.work.advance()
	switch x := rec.(type) {
	case graphbackup.NodeRecord:
		if !x.IsLease() {
			c.nodes++
		}
	case graphbackup.RelRecord:
		c.rels++
	}
	return rec, nil
}
