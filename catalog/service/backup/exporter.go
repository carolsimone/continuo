// Package backup exports each tenant's catalog graph to object storage on a
// schedule, keeps the newest exports, and restores an export. An export or a
// restore holds the tenant's export lease while it reads or writes the graph,
// so no catalog write lands meanwhile; the lease is renewed while the work
// runs and expires by itself if the process dies.
package backup

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/serialization/exportfile"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/google/uuid"
)

const (
	// leaseTTL is how long a lease lasts past its last renewal, so a holder
	// that crashed pauses the tenant's writes for at most this long.
	leaseTTL = 2 * time.Minute
	// leaseRenewEvery is how often a running export or restore renews its
	// lease: a renewal that fails is retried at the next tick, three times
	// before the lease can lapse.
	leaseRenewEvery = 30 * time.Second
	// releaseTimeout bounds the release of a lease, which runs even when the
	// work's context was cancelled.
	releaseTimeout = 10 * time.Second
)

// ErrSkipped means the export or restore did not start: another export or a
// restore holds the tenant's lease.
var ErrSkipped = errors.New("another export or restore holds the tenant's lease")

// Result is one stored export: its key, the size of the object, the records
// it holds and how long it held the tenant's lease, the time the tenant's
// writes were paused.
type Result struct {
	ObjectKey     string
	Bytes         int64
	Nodes         int64
	Relationships int64
	Duration      time.Duration
}

// Exporter exports one tenant's graph at a time as a gzipped export file,
// streamed to the backup store, and keeps the tenant's newest exports.
type Exporter struct {
	keeper  leaseKeeper
	graph   ports.GraphExporter
	store   ports.BackupStore
	metrics ports.BackupMetrics
	keep    int
	clock   func() time.Time
	logger  *slog.Logger
}

// NewExporter returns an Exporter that keeps the newest keep exports of each
// tenant. clock names each export's object.
func NewExporter(lease ports.ExportLease, graph ports.GraphExporter, store ports.BackupStore, metrics ports.BackupMetrics, keep int, clock func() time.Time, logger *slog.Logger) *Exporter {
	return &Exporter{
		keeper: newLeaseKeeper(lease, logger),
		graph:  graph, store: store, metrics: metrics, keep: keep, clock: clock, logger: logger,
	}
}

// ExportTenant exports tenantID's graph. It opens the object first, so a
// store that refuses writes fails the export before the tenant's writes are
// paused; then it takes the tenant's export lease, streams the graph through
// gzip into the object, confirms the lease is still held, completes the
// object and releases the lease. A held lease skips the export
// (ErrSkipped). Any failure aborts the upload, so nothing is stored, and
// releases the lease. After a stored export it deletes the tenant's exports
// beyond the newest keep. Every export is logged and observed with its
// outcome.
func (e *Exporter) ExportTenant(ctx context.Context, tenantID string) (Result, error) {
	res, err := e.export(ctx, tenantID)
	switch {
	case errors.Is(err, ErrSkipped):
		e.logger.Info("catalog export skipped: another export or restore holds the tenant's lease", "tenant_id", tenantID)
		e.metrics.ExportObserved(ports.ExportSkipped, 0, 0, 0, 0)
	case err != nil:
		level := slog.LevelError
		if ctx.Err() != nil {
			level = slog.LevelWarn
		}
		e.logger.Log(ctx, level, "catalog export failed: nothing was stored", "tenant_id", tenantID, "error", err)
		e.metrics.ExportObserved(ports.ExportFailure, 0, 0, 0, 0)
	default:
		e.logger.Info("catalog export stored", "tenant_id", tenantID, "object_key", res.ObjectKey,
			"bytes", res.Bytes, "nodes", res.Nodes, "relationships", res.Relationships,
			"duration_seconds", res.Duration.Seconds())
		e.prune(ctx, tenantID, res.ObjectKey)
		e.metrics.ExportObserved(ports.ExportSuccess, res.Duration, res.Bytes, res.Nodes, res.Relationships)
	}
	return res, err
}

func (e *Exporter) export(ctx context.Context, tenantID string) (Result, error) {
	w, key, err := e.store.Open(ctx, tenantID, e.clock())
	if err != nil {
		return Result{}, fmt.Errorf("open the export of tenant %q: %w", tenantID, err)
	}
	holder := newHolder(ports.LeaseExport)
	if err := e.keeper.lease.Acquire(ctx, tenantID, ports.LeaseExport, holder, e.keeper.ttl); err != nil {
		if errors.Is(err, ports.ErrLeaseHeld) {
			err = fmt.Errorf("export of tenant %q: %w", tenantID, ErrSkipped)
		} else {
			err = fmt.Errorf("take the export lease of tenant %q: %w", tenantID, err)
		}
		return Result{}, errors.Join(err, e.abort(w, key))
	}
	held := e.clock()
	res, err := e.stream(ctx, tenantID, holder, w)
	// The lease is released before a failed upload is aborted, so the
	// tenant's writes resume without waiting on the store.
	releaseErr := e.keeper.release(ctx, tenantID, holder)
	res.Duration = e.clock().Sub(held)
	if err != nil {
		return Result{}, errors.Join(err, e.abort(w, key), releaseErr)
	}
	if releaseErr != nil {
		e.logger.Error("catalog export stored, but its lease was not released: the tenant's writes resume when it expires",
			"tenant_id", tenantID, "object_key", key, "expires_within", e.keeper.ttl.String(), "error", releaseErr)
	}
	res.ObjectKey = key
	return res, nil
}

// stream writes the tenant's graph to w and completes it, while holder's
// lease is held and renewed. It completes w only once the lease is confirmed
// held after the read: no write can have changed the graph under it.
func (e *Exporter) stream(ctx context.Context, tenantID, holder string, w ports.BackupWriter) (Result, error) {
	heldCtx, stop := e.keeper.hold(ctx, tenantID, holder)
	sink := &countingWriter{w: w}
	zw := gzip.NewWriter(sink)
	records := &countingRecords{RecordWriter: exportfile.NewWriter(zw)}
	err := e.graph.Export(heldCtx, tenantID, records)
	lost := context.Cause(heldCtx)
	stop()
	switch {
	case errors.Is(lost, ports.ErrLeaseLost):
		return Result{}, fmt.Errorf("export of tenant %q stopped: %w", tenantID, lost)
	case err != nil:
		return Result{}, fmt.Errorf("export the graph of tenant %q: %w", tenantID, err)
	}
	if err := zw.Close(); err != nil {
		return Result{}, fmt.Errorf("compress the export of tenant %q: %w", tenantID, err)
	}
	if err := e.keeper.confirm(ctx, tenantID, holder); err != nil {
		return Result{}, err
	}
	if err := w.Close(); err != nil {
		return Result{}, fmt.Errorf("complete the export of tenant %q: %w", tenantID, err)
	}
	return Result{Bytes: sink.n, Nodes: records.nodes, Relationships: records.rels}, nil
}

// abort discards an upload that will not be completed. A failure is logged:
// the parts uploaded so far stay in the store until a bucket rule that
// aborts incomplete multipart uploads removes them.
func (e *Exporter) abort(w ports.BackupWriter, key string) error {
	if err := w.Abort(); err != nil {
		e.logger.Error("catalog export not aborted: its uploaded parts stay in the store", "object_key", key, "error", err)
		return fmt.Errorf("abort the export %s: %w", key, err)
	}
	return nil
}

// countingWriter counts the bytes written through it.
type countingWriter struct {
	w ports.BackupWriter
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// countingRecords counts the nodes and relationships written through it.
type countingRecords struct {
	ports.RecordWriter
	nodes, rels int64
}

func (c *countingRecords) WriteNode(n export.NodeRecord) error {
	if err := c.RecordWriter.WriteNode(n); err != nil {
		return err
	}
	c.nodes++
	return nil
}

func (c *countingRecords) WriteRel(r export.RelRecord) error {
	if err := c.RecordWriter.WriteRel(r); err != nil {
		return err
	}
	c.rels++
	return nil
}

// hostname names this process in a lease's holder.
var hostname = func() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}()

// newHolder returns a holder no other export or restore uses:
// "<kind>/<host>/<uuid>".
func newHolder(kind ports.LeaseKind) string {
	return fmt.Sprintf("%s/%s/%s", kind, hostname, uuid.NewString())
}

// leaseKeeper holds a tenant's export lease while an export or a restore
// runs.
type leaseKeeper struct {
	lease      ports.ExportLease
	ttl        time.Duration
	renewEvery time.Duration
	logger     *slog.Logger
}

func newLeaseKeeper(lease ports.ExportLease, logger *slog.Logger) leaseKeeper {
	return leaseKeeper{lease: lease, ttl: leaseTTL, renewEvery: leaseRenewEvery, logger: logger}
}

// hold renews holder's lease of tenantID every renewEvery until stop is
// called, and returns a context for the work the lease covers. A renewal
// that reports ErrLeaseLost cancels that context with it as the cause; any
// other failure is logged and retried at the next tick, since the lease
// lasts ttl past its last renewal. stop waits for the renewals to end and
// cancels the context.
func (k leaseKeeper) hold(ctx context.Context, tenantID, holder string) (context.Context, func()) {
	heldCtx, cancel := context.WithCancelCause(ctx)
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(k.renewEvery)
		defer tick.Stop()
		for {
			select {
			case <-quit:
				return
			case <-heldCtx.Done():
				return
			case <-tick.C:
			}
			err := k.lease.Renew(heldCtx, tenantID, holder, k.ttl)
			switch {
			case err == nil:
			case errors.Is(err, ports.ErrLeaseLost):
				k.logger.Error("export lease lost: stopping the work it covered", "tenant_id", tenantID, "holder", holder, "error", err)
				cancel(err)
				return
			case heldCtx.Err() == nil:
				k.logger.Warn("export lease renewal failed; retrying at the next tick", "tenant_id", tenantID, "holder", holder, "error", err)
			}
		}
	}()
	return heldCtx, func() {
		close(quit)
		<-done
		cancel(nil)
	}
}

// confirm renews holder's lease once more. Every renewal requires a live
// lease, so success proves the lease never lapsed since it was taken.
func (k leaseKeeper) confirm(ctx context.Context, tenantID, holder string) error {
	if err := k.lease.Renew(ctx, tenantID, holder, k.ttl); err != nil {
		return fmt.Errorf("confirm the export lease of tenant %q: %w", tenantID, err)
	}
	return nil
}

// release ends holder's lease on a context that outlives ctx's cancellation,
// so a shutdown still releases it.
func (k leaseKeeper) release(ctx context.Context, tenantID, holder string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if err := k.lease.Release(releaseCtx, tenantID, holder); err != nil {
		return fmt.Errorf("release the export lease of tenant %q: %w", tenantID, err)
	}
	return nil
}
