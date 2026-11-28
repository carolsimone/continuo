package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ContractHistorySweeper deletes the contract history that has outlived its
// retention, across every tenant: the :CONTRACT_PROMOTED edges a node's
// promotions drew, and the :ContractVersion nodes no edge and no pointer
// reaches once those edges are gone.
//
// A node's two newest edges, by the counter each carries, are never deleted
// whatever their age, and neither is the edge the node's :CURRENT_CONTRACT
// pointer names. In the graph a promotion writes, the newest edge is the
// current contract and the one below it is the version before it, so the
// sweep never deletes either. Because the newest edge stays, the counter a
// node's next change takes (one above its newest edge) is the same after a
// sweep as before it. The earliest version a revert names in same_as_version
// is the earliest the graph still holds: a revert to a contract the sweep
// deleted is recorded as a change that repeats no version.
//
// It is a system path, like MarkerSweeper: it reads which tenants hold
// deletable history, then sweeps each in a transaction of its own that takes
// the tenant's backup gate first and deletes nothing while a backup or a
// restore holds the tenant's lease.
type ContractHistorySweeper struct {
	client Neo4jClient
	logger *slog.Logger

	// listTenants and pruneOne are the sweeper's two reads of the graph; they
	// are fields so a test can drive Prune without a database.
	listTenants func(ctx context.Context, retentionMs int64) ([]string, error)
	pruneOne    func(ctx context.Context, tenantID string, retentionMs, limit int64) (int64, error)

	// failed holds the tenants whose prune failed in the sweep that is under
	// way, which skips them until AfterSweep.
	mu     sync.Mutex
	failed map[string]struct{}
}

// NewContractHistorySweeper returns a sweeper over client's graph.
func NewContractHistorySweeper(client Neo4jClient, logger *slog.Logger) *ContractHistorySweeper {
	s := &ContractHistorySweeper{client: client, logger: logger, failed: map[string]struct{}{}}
	s.listTenants = s.tenantsWithDeletableHistory
	s.pruneOne = s.pruneTenant
	return s
}

// deletableEdgesCypher binds e to each :CONTRACT_PROMOTED edge of the nodes it
// matches that the sweep may delete: older than $retention_ms on the database
// clock, below the node's two newest edges by version, and not the edge the
// node's pointer names. A node that holds two edges or fewer yields none:
// range(2, n - 1) is empty for n below 3. It names no label-qualified node,
// so the statements that embed it carry the tenant in their own patterns.
const deletableEdgesCypher = `
WITH n, e ORDER BY e.version DESC
WITH n, collect(e) AS edges
OPTIONAL MATCH (n)-[cur:CURRENT_CONTRACT]->()
WITH n, edges, cur.version AS current_version
UNWIND range(2, size(edges) - 1) AS rank
WITH n, edges[rank] AS e, current_version
WHERE e.promoted_at < datetime() - duration({milliseconds: $retention_ms})
  AND (current_version IS NULL OR e.version <> current_version)`

// deleteContractEdgesCypher deletes at most $limit of the tenant's deletable
// edges and reports how many.
const deleteContractEdgesCypher = `
MATCH (n:Node {tenant_id: $tenant_id})-[e:CONTRACT_PROMOTED]->(:ContractVersion {tenant_id: $tenant_id})` +
	deletableEdgesCypher + `
WITH e LIMIT $limit
DELETE e
RETURN count(*) AS deleted`

// deleteUnreachedVersionsCypher deletes at most $limit of the tenant's
// :ContractVersion nodes that none of the tenant's nodes reaches by a
// :CONTRACT_PROMOTED edge or a :CURRENT_CONTRACT pointer, and reports how
// many. It sees the edges the same transaction has just deleted.
const deleteUnreachedVersionsCypher = `
MATCH (v:ContractVersion {tenant_id: $tenant_id})
WHERE NOT EXISTS { MATCH (:Node {tenant_id: $tenant_id})-[:CONTRACT_PROMOTED|CURRENT_CONTRACT]->(v) }
WITH v LIMIT $limit
DETACH DELETE v
RETURN count(*) AS deleted`

// Prune deletes at most limit rows of contract history recorded more than
// retention ago on the database clock, and returns how many it deleted. A row
// is an edge or a version: each pass deletes edges first, then the versions
// they left unreached. The limit counts every tenant together: a pass visits
// the tenants that hold deletable history in turn until it has deleted limit.
// A tenant whose backup lease is held is skipped. A tenant whose sweep fails
// is logged and does not stop the others: it is skipped for the rest of the
// sweep, so it logs once however many passes the sweep takes, and its history
// waits for the next sweep. The count of what the pass deleted comes back
// without an error. A cancelled ctx ends the pass with ctx's error and the
// count so far, and logs nothing, since only a shutdown cancels it.
//
// It is a retention.PruneFunc (pkg/retention), so the sweeper calls it in a
// loop until a pass deletes fewer than limit or the sweep reaches its cap on
// batches, then calls AfterSweep, which forgets the failed tenants.
func (s *ContractHistorySweeper) Prune(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	retentionMs := retention.Milliseconds()
	if retentionMs < 1 {
		return 0, fmt.Errorf("neo4j: a contract history sweep needs a retention of at least 1ms, got %s", retention)
	}
	if limit < 1 {
		return 0, fmt.Errorf("neo4j: a contract history sweep needs a limit of at least 1, got %d", limit)
	}

	tenants, err := s.listTenants(ctx, retentionMs)
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, tenantID := range tenants {
		if deleted >= int64(limit) {
			break
		}
		if err = ctx.Err(); err != nil {
			return deleted, err
		}
		if s.hasFailed(tenantID) {
			continue
		}
		n, pruneErr := s.pruneOne(ctx, tenantID, retentionMs, int64(limit)-deleted)
		if pruneErr != nil {
			if err = ctx.Err(); err != nil {
				return deleted, err
			}
			s.markFailed(tenantID)
			s.logger.Error("contract history sweep of a tenant failed; its history waits for the next sweep",
				"tenant_id", tenantID, "error", pruneErr)
			continue
		}
		deleted += n
	}
	return deleted, nil
}

// AfterSweep ends the sweep the pruner has been running: the tenants whose
// prune failed in it are tried again in the next. pkg/retention calls it when
// a sweep ends, however it ends.
func (s *ContractHistorySweeper) AfterSweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.failed)
}

func (s *ContractHistorySweeper) hasFailed(tenantID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.failed[tenantID]
	return ok
}

func (s *ContractHistorySweeper) markFailed(tenantID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[tenantID] = struct{}{}
}

// pruneTenant deletes at most limit rows of the tenant's contract history in
// one write transaction behind the tenant's backup gate, and returns how many.
// It deletes nothing while the tenant's lease is held. Edges come first, up to
// limit, and the versions left unreached take what is left of it.
func (s *ContractHistorySweeper) pruneTenant(ctx context.Context, tenantID string, retentionMs, limit int64) (int64, error) {
	var deleted int64
	err := inWriteTx(ctx, s.client, tenantID, func(run TenantRunner) (bool, error) {
		gate, err := lockBackupGate(ctx, run)
		if err != nil {
			return false, err
		}
		if gate.held {
			s.logger.Debug("contract history sweep skipped: the tenant's backup lease is held", "tenant_id", tenantID)
			return false, nil
		}
		edges, err := deletedCount(ctx, run, deleteContractEdgesCypher,
			map[string]any{"retention_ms": retentionMs, "limit": limit})
		if err != nil {
			return false, fmt.Errorf("delete expired contract edges: %w", err)
		}
		var versions int64
		if edges < limit {
			versions, err = deletedCount(ctx, run, deleteUnreachedVersionsCypher,
				map[string]any{"limit": limit - edges})
			if err != nil {
				return false, fmt.Errorf("delete unreached contract versions: %w", err)
			}
		}
		if err := unlockBackupGate(ctx, run); err != nil {
			return false, err
		}
		deleted = edges + versions
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// deletedCount runs a statement that returns one row with the integer column
// deleted.
func deletedCount(ctx context.Context, run TenantRunner, cypher string, params map[string]any) (int64, error) {
	res, err := run.Run(ctx, cypher, params)
	if err != nil {
		return 0, err
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return 0, err
	}
	n, ok := rec.Values[0].(int64)
	if !ok {
		return 0, fmt.Errorf("neo4j: the deleted count is %T, want an integer", rec.Values[0])
	}
	return n, nil
}
