package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/carolsimone/continuo/catalog/service/graphbackup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
)

// The tenant's backup lease lives on its (:Meta {tenant_id, key:'backup'})
// node: holder, kind, and until, a datetime on the database clock. The lease
// is held while until lies ahead of datetime().
//
// The same node is the tenant's backup gate. Every write transaction the unit
// of work runs sets _lock on it before any other statement, which takes the
// node's exclusive lock until the transaction ends, and reads the lease in the
// same statement; it removes _lock again before it commits, so the property
// never persists. Because the gate is the first lock such a transaction takes,
// it cannot deadlock with a lock the transaction takes after it, such as the
// swap's lock on the current_release node. An Acquire takes the same lock, so
// it waits for the tenant's in-flight writes to commit, and every write that
// locks the node after the lease is taken sees it held.

// A message whose writes span several transactions (a non-final batch of the
// code versions) also marks itself on the lease node, between two of its
// batches. inflight_message_ids lists the marks, one per consumer group and
// message, and inflight_untils lists, in the same order, the datetime on the
// database clock at which each lapses. A mark is live while its lapse time lies
// ahead of datetime(), so each message lapses on its own clock and a later
// message's batches never extend an earlier one's. A backup's Acquire refuses
// while any mark is live, so a backup never starts between a message's
// batches; a crashed handler's mark lapses on its own. A write never waits for
// a mark.

// inflightTTL is how long a non-final write keeps its message's mark live.
const inflightTTL = uow.InFlightTTL

// inflightMarkCypher adds a mark, once, and moves its lapse time to ttl from
// now. It drops the marks that have lapsed, so an abandoned message does not
// stay on the list.
const inflightMarkCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
WITH m, coalesce(m.inflight_message_ids, []) AS ids, coalesce(m.inflight_untils, []) AS untils
WITH m, ids, untils,
     [i IN range(0, size(ids) - 1) WHERE ids[i] <> $mark AND untils[i] > datetime()] AS keep
SET m.inflight_message_ids = [i IN keep | ids[i]] + [$mark],
    m.inflight_untils = [i IN keep | untils[i]] + [datetime() + duration({milliseconds: $ttl_ms})]`

// inflightClearCypher removes a mark, and both lists once none is left. A
// tenant with no mark is left as it is.
const inflightClearCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
WHERE m.inflight_message_ids IS NOT NULL
WITH m, m.inflight_message_ids AS ids, m.inflight_untils AS untils
WITH m, ids, untils, [i IN range(0, size(ids) - 1) WHERE ids[i] <> $mark] AS keep
SET m.inflight_message_ids = CASE WHEN size(keep) = 0 THEN null ELSE [i IN keep | ids[i]] END,
    m.inflight_untils = CASE WHEN size(keep) = 0 THEN null ELSE [i IN keep | untils[i]] END`

// backupGateCypher locks the tenant's lease node, creating it on the tenant's
// first write, and reports whether a lease is held and whether a message's
// in-flight mark is live. SET takes the lock before RETURN reads until, so the
// read sees the lease and the mark as the last holder of the lock committed
// them.
const backupGateCypher = `
MERGE (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m._lock = true
RETURN m.until IS NOT NULL AND m.until > datetime() AS held,
       any(u IN coalesce(m.inflight_untils, []) WHERE u > datetime()) AS inflight`

// backupGateUnlockCypher removes the gate's lock property before commit.
const backupGateUnlockCypher = `MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key}) REMOVE m._lock`

// leaseHeldCypher reads, without locking, whether the tenant's lease is live. A
// tenant with no lease node returns no row.
const leaseHeldCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
RETURN m.until IS NOT NULL AND m.until > datetime() AS held`

// leaseLockCypher locks an existing lease node and reports its holder and
// whether its lease is live, for a renewal or a release.
const leaseLockCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m._lock = true
RETURN m.holder AS holder, m.until IS NOT NULL AND m.until > datetime() AS live`

// leaseTakeCypher records a new lease on the locked node and unlocks it.
const leaseTakeCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m.holder = $holder, m.kind = $kind, m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock`

// leaseTakeBackupCypher records a new backup lease like leaseTakeCypher and
// removes the in-flight mark, which Acquire has found lapsed or absent.
const leaseTakeBackupCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m.holder = $holder, m.kind = $kind, m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock, m.inflight_message_ids, m.inflight_untils`

// leaseExtendCypher moves a renewed lease's until and unlocks the node.
const leaseExtendCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m.until = datetime() + duration({milliseconds: $ttl_ms})
REMOVE m._lock`

// leaseClearCypher ends a lease and unlocks the node.
const leaseClearCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
REMOVE m.holder, m.kind, m.until, m._lock`

// leaseParams returns params with $lease_key, the key of the tenant's backup
// lease node, which every statement on that node binds.
func leaseParams(params map[string]any) map[string]any {
	return leaseNodeParams(graphbackup.LeaseNodeKey, params)
}

// leaseNodeParams returns params with $lease_key set to key, the key of the
// tenant's (:Meta) node a lease statement acts on: the backup lease node or
// the live-folder node.
func leaseNodeParams(key string, params map[string]any) map[string]any {
	out := make(map[string]any, len(params)+1)
	maps.Copy(out, params)
	out["lease_key"] = key
	return out
}

// gateState is what a transaction reads from the tenant's lease node under the
// gate's lock.
type gateState struct {
	// held is true while a backup or a restore holds the lease.
	held bool
	// inflight is true while a message sits between two of its writes.
	inflight bool
}

// lockBackupGate takes the tenant's backup gate on the open transaction and
// reads the lease and the in-flight mark.
func lockBackupGate(ctx context.Context, run TenantRunner) (gateState, error) {
	gate, err := lockLeaseGate(ctx, run, graphbackup.LeaseNodeKey)
	if err != nil {
		return gateState{}, fmt.Errorf("lock backup gate: %w", err)
	}
	return gate, nil
}

// lockLeaseGate locks the tenant's (:Meta) node of key on the open
// transaction, creating it when missing, and reads its lease and in-flight
// mark.
func lockLeaseGate(ctx context.Context, run TenantRunner, key string) (gateState, error) {
	res, err := run.Run(ctx, backupGateCypher, leaseNodeParams(key, nil))
	if err != nil {
		return gateState{}, err
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return gateState{}, err
	}
	held, _ := rec.Get("held")
	inflight, _ := rec.Get("inflight")
	heldB, heldOK := held.(bool)
	inflightB, inflightOK := inflight.(bool)
	if !heldOK || !inflightOK {
		return gateState{}, fmt.Errorf("columns are %T and %T, want booleans", held, inflight)
	}
	return gateState{held: heldB, inflight: inflightB}, nil
}

// markInFlight adds mark to the in-flight marks on the locked lease node.
func markInFlight(ctx context.Context, run TenantRunner, mark string) error {
	if err := execCypher(ctx, run, inflightMarkCypher, leaseParams(map[string]any{
		"mark": mark, "ttl_ms": inflightTTL.Milliseconds(),
	})); err != nil {
		return fmt.Errorf("mark message in flight: %w", err)
	}
	return nil
}

// clearInFlight removes mark from the in-flight marks on the locked lease
// node.
func clearInFlight(ctx context.Context, run TenantRunner, mark string) error {
	if err := execCypher(ctx, run, inflightClearCypher, leaseParams(map[string]any{"mark": mark})); err != nil {
		return fmt.Errorf("clear message in flight: %w", err)
	}
	return nil
}

// unlockBackupGate removes the gate's lock property; the lock itself is
// released when the transaction ends.
func unlockBackupGate(ctx context.Context, run TenantRunner) error {
	if err := execCypher(ctx, run, backupGateUnlockCypher, leaseParams(nil)); err != nil {
		return fmt.Errorf("unlock backup gate: %w", err)
	}
	return nil
}

// backupLease implements ports.BackupLease on each tenant's lease node.
type backupLease struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ ports.BackupLease = (*backupLease)(nil)

// NewBackupLease returns the backup lease kept in client's graph.
func NewBackupLease(client Neo4jClient, logger *slog.Logger) ports.BackupLease {
	return &backupLease{client: client, logger: logger}
}

// Acquire takes the tenant's lease for holder, after the tenant's in-flight
// writes commit. A live lease, whoever holds it, is ErrLeaseHeld; an expired
// one is taken over. A backup also refuses with ErrWritesInFlight while a
// message's in-flight mark is live, and removes a mark that has lapsed; a
// restore ignores the mark.
func (l *backupLease) Acquire(ctx context.Context, tenantID string, kind ports.LeaseKind, holder string, ttl time.Duration) error {
	if kind != ports.LeaseBackup && kind != ports.LeaseRestore {
		return fmt.Errorf("neo4j: unknown backup lease kind %q", kind)
	}
	ttlMs, err := leaseRequest(tenantID, holder, ttl)
	if err != nil {
		return err
	}
	err = inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		gate, err := lockBackupGate(ctx, run)
		if err != nil {
			return false, err
		}
		if gate.held {
			return false, fmt.Errorf("tenant %q: %w", tenantID, ports.ErrLeaseHeld)
		}
		take := leaseTakeCypher
		if kind == ports.LeaseBackup {
			if gate.inflight {
				return false, fmt.Errorf("tenant %q: %w", tenantID, ports.ErrWritesInFlight)
			}
			take = leaseTakeBackupCypher
		}
		if err := execCypher(ctx, run, take, leaseParams(map[string]any{
			"holder": holder, "kind": string(kind), "ttl_ms": ttlMs,
		})); err != nil {
			return false, fmt.Errorf("take backup lease: %w", err)
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	l.logger.Info("backup lease acquired", "tenant_id", tenantID, "kind", string(kind), "holder", holder, "ttl", ttl.String())
	return nil
}

// Renew extends holder's live lease to ttl from now. A lease another holder
// owns, an expired lease and a tenant with no lease are all ErrLeaseLost: once
// the lease lapsed, writes may have landed since.
func (l *backupLease) Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error {
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
		if err := execCypher(ctx, run, leaseExtendCypher, leaseParams(map[string]any{"ttl_ms": ttlMs})); err != nil {
			return false, fmt.Errorf("renew backup lease: %w", err)
		}
		return true, nil
	})
}

// Release ends holder's lease, live or expired. A lease another holder owns,
// and a tenant with no lease, are left as they are.
func (l *backupLease) Release(ctx context.Context, tenantID, holder string) error {
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
		if err := execCypher(ctx, run, leaseClearCypher, leaseParams(nil)); err != nil {
			return false, fmt.Errorf("release backup lease: %w", err)
		}
		released = true
		return true, nil
	})
	if err != nil {
		return err
	}
	if released {
		l.logger.Info("backup lease released", "tenant_id", tenantID, "holder", holder)
	}
	return nil
}

// lockLease locks the tenant's backup lease node and reads its holder and
// whether its lease is live; found is false when the tenant has no lease node.
func lockLease(ctx context.Context, run TenantRunner) (holder string, live, found bool, err error) {
	return lockLeaseNode(ctx, run, graphbackup.LeaseNodeKey)
}

// lockLeaseNode locks the tenant's existing (:Meta) node of key and reads its
// lease's holder and whether the lease is live; found is false when the
// tenant has no such node.
func lockLeaseNode(ctx context.Context, run TenantRunner, key string) (holder string, live, found bool, err error) {
	res, err := run.Run(ctx, leaseLockCypher, leaseNodeParams(key, nil))
	if err != nil {
		return "", false, false, fmt.Errorf("lock lease node %q: %w", key, err)
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return "", false, false, fmt.Errorf("lock lease node %q: %w", key, err)
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
		return errors.New("neo4j: a lease needs a holder")
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
		return 0, fmt.Errorf("neo4j: a lease needs a ttl of at least 1ms, got %s", ttl)
	}
	return ttlMs, nil
}
