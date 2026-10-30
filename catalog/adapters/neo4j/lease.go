package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

// The tenant's export lease lives on its (:Meta {tenant_id, key:'export'})
// node: holder, kind, and until, a datetime on the database clock. The lease
// is held while until lies ahead of datetime().
//
// The same node is the tenant's export gate. Every write transaction the unit
// of work runs sets _lock on it before any other statement, which takes the
// node's exclusive lock until the transaction ends, and reads the lease in the
// same statement; it removes _lock again before it commits, so the property
// never persists. Because the gate is the first lock such a transaction takes,
// it cannot deadlock with a lock the transaction takes after it, such as the
// swap's lock on the current_release node. An Acquire takes the same lock, so
// it waits for the tenant's in-flight writes to commit, and every write that
// locks the node after the lease is taken sees it held.

// exportGateCypher locks the tenant's lease node, creating it on the tenant's
// first write, and reports whether a lease is held. SET takes the lock before
// RETURN reads until, so the read sees the lease as the last holder of the
// lock committed it.
const exportGateCypher = `MERGE (m:Meta {tenant_id: $tenant_id, key: 'export'}) SET m._lock = true RETURN m.until IS NOT NULL AND m.until > datetime() AS held`

// exportGateUnlockCypher removes the gate's lock property before commit.
const exportGateUnlockCypher = `MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) REMOVE m._lock`

// leaseLockCypher locks an existing lease node and reports its holder and
// whether its lease is live, for a renewal or a release.
const leaseLockCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m._lock = true
RETURN m.holder AS holder, m.until IS NOT NULL AND m.until > datetime() AS live`

// leaseTakeCypher records a new lease on the locked node and unlocks it.
const leaseTakeCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m.holder = $holder, m.kind = $kind, m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock`

// leaseExtendCypher moves a renewed lease's until and unlocks the node.
const leaseExtendCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock`

// leaseClearCypher ends a lease and unlocks the node.
const leaseClearCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
REMOVE m.holder, m.kind, m.until, m._lock`

// lockExportGate takes the tenant's export gate on the open transaction and
// reports whether an export or a restore holds the lease.
func lockExportGate(ctx context.Context, run TenantRunner) (held bool, err error) {
	held, err = singleBool(ctx, run, exportGateCypher, nil, "held")
	if err != nil {
		return false, fmt.Errorf("lock export gate: %w", err)
	}
	return held, nil
}

// unlockExportGate removes the gate's lock property; the lock itself is
// released when the transaction ends.
func unlockExportGate(ctx context.Context, run TenantRunner) error {
	if err := execCypher(ctx, run, exportGateUnlockCypher, nil); err != nil {
		return fmt.Errorf("unlock export gate: %w", err)
	}
	return nil
}

// exportLease implements ports.ExportLease on each tenant's lease node.
type exportLease struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ ports.ExportLease = (*exportLease)(nil)

// NewExportLease returns the export lease kept in client's graph.
func NewExportLease(client Neo4jClient, logger *slog.Logger) ports.ExportLease {
	return &exportLease{client: client, logger: logger}
}

// Acquire takes the tenant's lease for holder, after the tenant's in-flight
// writes commit. A live lease, whoever holds it, is ErrLeaseHeld; an expired
// one is taken over.
func (l *exportLease) Acquire(ctx context.Context, tenantID string, kind ports.LeaseKind, holder string, ttl time.Duration) error {
	if kind != ports.LeaseExport && kind != ports.LeaseRestore {
		return fmt.Errorf("neo4j: unknown export lease kind %q", kind)
	}
	ttlMs, err := leaseRequest(tenantID, holder, ttl)
	if err != nil {
		return err
	}
	err = inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		held, err := lockExportGate(ctx, run)
		if err != nil {
			return false, err
		}
		if held {
			return false, fmt.Errorf("tenant %q: %w", tenantID, ports.ErrLeaseHeld)
		}
		if err := execCypher(ctx, run, leaseTakeCypher, map[string]any{
			"holder": holder, "kind": string(kind), "ttl_ms": ttlMs,
		}); err != nil {
			return false, fmt.Errorf("take export lease: %w", err)
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	l.logger.Info("export lease acquired", "tenant_id", tenantID, "kind", string(kind), "holder", holder, "ttl", ttl.String())
	return nil
}

// Renew extends holder's live lease to ttl from now. A lease another holder
// owns, an expired lease and a tenant with no lease are all ErrLeaseLost: once
// the lease lapsed, writes may have landed since.
func (l *exportLease) Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error {
	ttlMs, err := leaseRequest(tenantID, holder, ttl)
	if err != nil {
		return err
	}
	return inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		owner, live, found, err := lockLease(ctx, run)
		if err != nil {
			return false, err
		}
		if !found || !live || owner != holder {
			return false, fmt.Errorf("tenant %q, holder %q: %w", tenantID, holder, ports.ErrLeaseLost)
		}
		if err := execCypher(ctx, run, leaseExtendCypher, map[string]any{"ttl_ms": ttlMs}); err != nil {
			return false, fmt.Errorf("renew export lease: %w", err)
		}
		return true, nil
	})
}

// Release ends holder's lease, live or expired. A lease another holder owns,
// and a tenant with no lease, are left as they are.
func (l *exportLease) Release(ctx context.Context, tenantID, holder string) error {
	if err := leaseParties(tenantID, holder); err != nil {
		return err
	}
	released := false
	err := inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		owner, _, found, err := lockLease(ctx, run)
		if err != nil {
			return false, err
		}
		if !found || owner != holder {
			return false, nil
		}
		if err := execCypher(ctx, run, leaseClearCypher, nil); err != nil {
			return false, fmt.Errorf("release export lease: %w", err)
		}
		released = true
		return true, nil
	})
	if err != nil {
		return err
	}
	if released {
		l.logger.Info("export lease released", "tenant_id", tenantID, "holder", holder)
	}
	return nil
}

// lockLease locks the tenant's lease node and reads its holder and whether its
// lease is live; found is false when the tenant has no lease node.
func lockLease(ctx context.Context, run TenantRunner) (holder string, live, found bool, err error) {
	res, err := run.Run(ctx, leaseLockCypher, nil)
	if err != nil {
		return "", false, false, fmt.Errorf("lock export lease: %w", err)
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return "", false, false, fmt.Errorf("lock export lease: %w", err)
	}
	if len(recs) == 0 {
		return "", false, false, nil
	}
	h, _ := recs[0].Get("holder")
	holder, _ = h.(string)
	l, _ := recs[0].Get("live")
	live, _ = l.(bool)
	return holder, live, true, nil
}

// leaseParties checks the tenant and holder every lease call names.
func leaseParties(tenantID, holder string) error {
	if tenantID == "" {
		return errEmptyTenant
	}
	if holder == "" {
		return errors.New("neo4j: an export lease needs a holder")
	}
	return nil
}

// leaseRequest checks the arguments of a call that sets a lease's until and
// returns ttl in whole milliseconds, the unit until is computed in.
func leaseRequest(tenantID, holder string, ttl time.Duration) (int64, error) {
	if err := leaseParties(tenantID, holder); err != nil {
		return 0, err
	}
	ttlMs := ttl.Milliseconds()
	if ttlMs < 1 {
		return 0, fmt.Errorf("neo4j: an export lease needs a ttl of at least 1ms, got %s", ttl)
	}
	return ttlMs, nil
}
