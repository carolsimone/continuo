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

// A message whose writes span several transactions (a non-final batch of the
// code versions) also marks itself on the lease node: inflight_message_ids lists
// the ids of the messages between two of their batches, and inflight_until, a
// datetime on the database clock, is the time the mark lapses. The mark is
// live while the list is not empty and inflight_until lies ahead of
// datetime(). An export's Acquire refuses while it is live, so an export never
// starts between a message's batches; a crashed handler's mark lapses on its
// own. A write never waits for the mark.

// inflightTTL is how long a non-final write keeps its message's mark live. It
// exceeds the time one batch takes, and a message that dies mid-way holds an
// export back for at most this long after its last batch.
const inflightTTL = 2 * time.Minute

// inflightMarkCypher adds a message to the in-flight list, once, and moves the
// mark's lapse time to ttl from now.
const inflightMarkCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m.inflight_message_ids = CASE
      WHEN $message_id IN coalesce(m.inflight_message_ids, []) THEN m.inflight_message_ids
      ELSE coalesce(m.inflight_message_ids, []) + $message_id END,
    m.inflight_until = datetime() + duration({milliseconds: $ttl_ms})`

// inflightClearCypher removes a message from the in-flight list, and the
// mark's lapse time once the list is empty. A tenant with no mark is left as
// it is.
const inflightClearCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
WHERE m.inflight_message_ids IS NOT NULL
WITH m, [id IN m.inflight_message_ids WHERE id <> $message_id] AS rest
SET m.inflight_message_ids = CASE WHEN size(rest) = 0 THEN null ELSE rest END,
    m.inflight_until = CASE WHEN size(rest) = 0 THEN null ELSE m.inflight_until END`

// exportGateCypher locks the tenant's lease node, creating it on the tenant's
// first write, and reports whether a lease is held and whether a message's
// in-flight mark is live. SET takes the lock before RETURN reads until, so the
// read sees the lease and the mark as the last holder of the lock committed
// them.
const exportGateCypher = `
MERGE (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m._lock = true
RETURN m.until IS NOT NULL AND m.until > datetime() AS held,
       coalesce(size(m.inflight_message_ids), 0) > 0 AND m.inflight_until IS NOT NULL AND m.inflight_until > datetime() AS inflight`

// exportGateUnlockCypher removes the gate's lock property before commit.
const exportGateUnlockCypher = `MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'}) REMOVE m._lock`

// leaseHeldCypher reads, without locking, whether the tenant's lease is live. A
// tenant with no lease node returns no row.
const leaseHeldCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
RETURN m.until IS NOT NULL AND m.until > datetime() AS held`

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

// leaseTakeExportCypher records a new export lease like leaseTakeCypher and
// removes the in-flight mark, which Acquire has found lapsed or absent.
const leaseTakeExportCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m.holder = $holder, m.kind = $kind, m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock, m.inflight_message_ids, m.inflight_until`

// leaseExtendCypher moves a renewed lease's until and unlocks the node.
const leaseExtendCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
SET m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock`

// leaseClearCypher ends a lease and unlocks the node.
const leaseClearCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'export'})
REMOVE m.holder, m.kind, m.until, m._lock`

// gateState is what a transaction reads from the tenant's lease node under the
// gate's lock.
type gateState struct {
	// held is true while an export or a restore holds the lease.
	held bool
	// inflight is true while a message sits between two of its writes.
	inflight bool
}

// lockExportGate takes the tenant's export gate on the open transaction and
// reads the lease and the in-flight mark.
func lockExportGate(ctx context.Context, run TenantRunner) (gateState, error) {
	res, err := run.Run(ctx, exportGateCypher, nil)
	if err != nil {
		return gateState{}, fmt.Errorf("lock export gate: %w", err)
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return gateState{}, fmt.Errorf("lock export gate: %w", err)
	}
	held, _ := rec.Get("held")
	inflight, _ := rec.Get("inflight")
	heldB, heldOK := held.(bool)
	inflightB, inflightOK := inflight.(bool)
	if !heldOK || !inflightOK {
		return gateState{}, fmt.Errorf("lock export gate: columns are %T and %T, want booleans", held, inflight)
	}
	return gateState{held: heldB, inflight: inflightB}, nil
}

// markInFlight lists messageID as in flight on the locked lease node.
func markInFlight(ctx context.Context, run TenantRunner, messageID string) error {
	if err := execCypher(ctx, run, inflightMarkCypher, map[string]any{
		"message_id": messageID, "ttl_ms": inflightTTL.Milliseconds(),
	}); err != nil {
		return fmt.Errorf("mark message in flight: %w", err)
	}
	return nil
}

// clearInFlight removes messageID from the in-flight list on the locked lease
// node.
func clearInFlight(ctx context.Context, run TenantRunner, messageID string) error {
	if err := execCypher(ctx, run, inflightClearCypher, map[string]any{"message_id": messageID}); err != nil {
		return fmt.Errorf("clear message in flight: %w", err)
	}
	return nil
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
// one is taken over. An export also refuses with ErrWritesInFlight while a
// message's in-flight mark is live, and removes a mark that has lapsed; a
// restore ignores the mark.
func (l *exportLease) Acquire(ctx context.Context, tenantID string, kind ports.LeaseKind, holder string, ttl time.Duration) error {
	if kind != ports.LeaseExport && kind != ports.LeaseRestore {
		return fmt.Errorf("neo4j: unknown export lease kind %q", kind)
	}
	ttlMs, err := leaseRequest(tenantID, holder, ttl)
	if err != nil {
		return err
	}
	err = inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		gate, err := lockExportGate(ctx, run)
		if err != nil {
			return false, err
		}
		if gate.held {
			return false, fmt.Errorf("tenant %q: %w", tenantID, ports.ErrLeaseHeld)
		}
		take := leaseTakeCypher
		if kind == ports.LeaseExport {
			if gate.inflight {
				return false, fmt.Errorf("tenant %q: %w", tenantID, ports.ErrWritesInFlight)
			}
			take = leaseTakeExportCypher
		}
		if err := execCypher(ctx, run, take, map[string]any{
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
