package backup

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

// ErrPartialRestore means a restore wiped the tenant's graph and did not
// finish loading the export.
var ErrPartialRestore = errors.New("the tenant's graph is partial and its schema migration has not run; " +
	"run the restore again: it wipes the tenant's graph first, so a second run repairs it")

// RestoreResult is what a restore loaded: the node and relationship records of
// the export.
type RestoreResult struct {
	Nodes         int64
	Relationships int64
}

// Restorer returns a tenant's graph to one of its exports.
type Restorer struct {
	keeper leaseKeeper
	graph  ports.GraphImporter
	store  ports.BackupStore
	logger *slog.Logger
}

// NewRestorer returns a Restorer that loads exports from store into graph.
func NewRestorer(lease ports.ExportLease, graph ports.GraphImporter, store ports.BackupStore, logger *slog.Logger) *Restorer {
	return &Restorer{keeper: newLeaseKeeper(lease, logger), graph: graph, store: store, logger: logger}
}

// Restore returns the graph of the tenant key names to the export stored at
// key. An export file is gzipped JSON lines: a header, then one line per node
// and one per relationship, each property value in its own type (a datetime
// as RFC 3339 text with an optional [zone] suffix).
//
// It streams the object twice and never holds it whole. The first read checks
// the whole file (exportfile.Validate) with no lease and no write, so a file
// that cannot be loaded exactly, or that names another tenant, leaves the
// graph untouched. Then it takes the tenant's export lease (ErrSkipped while
// another export or restore holds it), opens the object again and checks its
// header against the first read's before it wipes the tenant's graph, loads
// the export, confirms the lease is still held and releases it. While it
// holds the lease, a step that moves no record and no byte for stallLimit (a
// database or store that never answers) stops it with ErrStalled and
// releases the lease. A failure after the wipe returns ErrPartialRestore.
func (r *Restorer) Restore(ctx context.Context, key string) (RestoreResult, error) {
	tenantID, _, err := export.ParseObjectKey(key)
	if err != nil {
		return RestoreResult{}, err
	}
	header, err := r.validate(ctx, key, tenantID)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("the export %s cannot be restored, and the graph of tenant %q is untouched: %w", key, tenantID, err)
	}
	holder := newHolder(ports.LeaseRestore)
	if err := r.keeper.lease.Acquire(ctx, tenantID, ports.LeaseRestore, holder, r.keeper.ttl); err != nil {
		if errors.Is(err, ports.ErrLeaseHeld) {
			return RestoreResult{}, fmt.Errorf("restore of tenant %q: %w", tenantID, ErrSkipped)
		}
		return RestoreResult{}, fmt.Errorf("take the export lease of tenant %q: %w", tenantID, err)
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

// validate reads the whole object and returns its header when it is an
// export tenantID's restore can load exactly.
func (r *Restorer) validate(ctx context.Context, key, tenantID string) (export.Header, error) {
	rc, err := r.store.Read(ctx, key)
	if err != nil {
		return export.Header{}, err
	}
	defer func() { _ = rc.Close() }()
	zr, err := gzip.NewReader(rc)
	if err != nil {
		return export.Header{}, fmt.Errorf("decompress: %w", err)
	}
	return exportfile.Validate(zr, tenantID)
}

// load opens the object again, checks it is the one validate read, wipes the
// tenant's graph and imports the object, while holder's lease is held,
// renewed and watched. Every byte read from the object and every record
// read is progress, and so is the start of the wipe and of the import, which
// move neither while the database works.
func (r *Restorer) load(ctx context.Context, tenantID, holder, key string, validated export.Header) (RestoreResult, error) {
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := &progress{}
	stop := r.keeper.watch(workCtx, cancel, tenantID, holder, work)
	defer stop()
	rc, err := r.store.Read(workCtx, key)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("the export %s cannot be read again, and the graph of tenant %q is untouched: %w", key, tenantID, err)
	}
	defer func() { _ = rc.Close() }()
	in, err := openExport(&progressReader{r: rc, work: work})
	if err == nil && !sameHeader(in.Header(), validated) {
		err = errors.New("its header differs from the one validated: the object changed")
	}
	if err != nil {
		return RestoreResult{}, fmt.Errorf("the export %s changed since it was validated, and the graph of tenant %q is untouched: %w", key, tenantID, err)
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
	if err := r.graph.Import(workCtx, tenantID, counted); err != nil {
		return RestoreResult{}, partial("load the export", err)
	}
	if cause := stopCause(workCtx); cause != nil {
		return RestoreResult{}, partial("load the export", cause)
	}
	if err := r.keeper.confirm(workCtx, tenantID, holder); err != nil {
		return RestoreResult{}, fmt.Errorf("restore of tenant %q from %s: the lease may have lapsed while the export loaded, "+
			"so writes may have landed in the graph during the restore; run the restore again: %w", tenantID, key, err)
	}
	return RestoreResult{Nodes: counted.nodes, Relationships: counted.rels}, nil
}

// openExport reads the gzip and export headers of an export stream.
func openExport(r io.Reader) (*exportfile.Reader, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	return exportfile.NewReader(zr)
}

// sameHeader reports whether two export headers are equal.
func sameHeader(a, b export.Header) bool {
	return a.FormatVersion == b.FormatVersion && a.TenantID == b.TenantID && a.ExportedAt.Equal(b.ExportedAt) &&
		a.CatalogVersion == b.CatalogVersion && maps.Equal(a.Nodes, b.Nodes) && maps.Equal(a.Relationships, b.Relationships)
}

// progressReader reads through r, each read that moves bytes being progress.
type progressReader struct {
	r    io.Reader
	work *progress
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.work.advance()
	}
	return n, err
}

// countingReader counts the node and relationship records read through it,
// each record being progress; an export lease node in the export, which a
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
	case export.NodeRecord:
		if !x.IsLease() {
			c.nodes++
		}
	case export.RelRecord:
		c.rels++
	}
	return rec, nil
}
