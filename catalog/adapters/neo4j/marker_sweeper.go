package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// MarkerSweeper deletes the processed-message markers that have outlived
// their retention, across every tenant. A marker only has to outlast the
// longest a message can still be redelivered, so the sweep keeps the graph
// from growing by one node per consumed message.
//
// It is a system path: it reads which tenants hold expired markers, then
// sweeps each in a transaction of its own. That transaction takes the
// tenant's export gate first, as every write does, and sweeps nothing while
// an export or a restore holds the tenant's lease. Because it takes the gate,
// it waits for the tenant's in-flight writes, and no lease can be taken
// between its reading the lease and its commit.
type MarkerSweeper struct {
	client Neo4jClient
	logger *slog.Logger
}

// NewMarkerSweeper returns a sweeper over client's graph.
func NewMarkerSweeper(client Neo4jClient, logger *slog.Logger) *MarkerSweeper {
	return &MarkerSweeper{client: client, logger: logger}
}

// expiredMarkerTenantsCypher lists the tenants that hold a marker recorded
// more than $retention_ms milliseconds ago on the database clock. A marker
// with no processed_at is never expired.
const expiredMarkerTenantsCypher = `
MATCH (p:ProcessedMessage)
WHERE p.tenant_id IS NOT NULL AND p.processed_at < datetime() - duration({milliseconds: $retention_ms})
RETURN DISTINCT p.tenant_id AS tenant_id
ORDER BY tenant_id`

// markerPruneCypher deletes at most $limit of the tenant's expired markers and
// reports how many.
const markerPruneCypher = `
MATCH (p:ProcessedMessage {tenant_id: $tenant_id})
WHERE p.processed_at < datetime() - duration({milliseconds: $retention_ms})
WITH p LIMIT $limit
DETACH DELETE p
RETURN count(*) AS deleted`

// Prune deletes at most limit markers recorded more than retention ago on the
// database clock, and returns how many it deleted. The limit counts every
// tenant together: a pass visits the tenants that hold expired markers in
// turn until it has deleted limit. A tenant whose export lease is held is
// skipped. A tenant whose sweep fails does not stop the others; the errors
// come back together with the count of what was deleted.
//
// It is a pkgoutbox.PruneFunc, so the retention sweeper calls it in a loop
// until a pass deletes fewer than limit.
func (s *MarkerSweeper) Prune(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	retentionMs := retention.Milliseconds()
	if retentionMs < 1 {
		return 0, fmt.Errorf("neo4j: a marker sweep needs a retention of at least 1ms, got %s", retention)
	}
	if limit < 1 {
		return 0, fmt.Errorf("neo4j: a marker sweep needs a limit of at least 1, got %d", limit)
	}

	tenants, err := s.tenantsWithExpiredMarkers(ctx, retentionMs)
	if err != nil {
		return 0, err
	}
	var deleted int64
	var failures []error
	for _, tenantID := range tenants {
		if deleted >= int64(limit) {
			break
		}
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		n, err := s.pruneTenant(ctx, tenantID, retentionMs, int64(limit)-deleted)
		if err != nil {
			failures = append(failures, fmt.Errorf("sweep processed-message markers of tenant %q: %w", tenantID, err))
			continue
		}
		deleted += n
	}
	return deleted, errors.Join(failures...)
}

// tenantsWithExpiredMarkers lists, in order, the tenants that hold a marker
// older than retentionMs.
func (s *MarkerSweeper) tenantsWithExpiredMarkers(ctx context.Context, retentionMs int64) ([]string, error) {
	session := s.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	tenants, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) ([]string, error) {
		res, err := tx.Run(ctx, expiredMarkerTenantsCypher, map[string]any{"retention_ms": retentionMs})
		if err != nil {
			return nil, err
		}
		recs, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(recs))
		for _, rec := range recs {
			if tenantID, _ := rec.Values[0].(string); tenantID != "" {
				out = append(out, tenantID)
			}
		}
		return out, nil
	})
	if err != nil {
		return nil, fmt.Errorf("list tenants with expired processed-message markers: %w", err)
	}
	return tenants, nil
}

// pruneTenant deletes at most limit of the tenant's expired markers in one
// write transaction behind the tenant's export gate, and returns how many. It
// deletes nothing while the tenant's lease is held.
func (s *MarkerSweeper) pruneTenant(ctx context.Context, tenantID string, retentionMs, limit int64) (int64, error) {
	var deleted int64
	err := inWriteTx(ctx, s.client, tenantID, func(run TenantRunner) (bool, error) {
		held, err := lockExportGate(ctx, run)
		if err != nil {
			return false, err
		}
		if held {
			s.logger.Debug("marker sweep skipped: the tenant's export lease is held", "tenant_id", tenantID)
			return false, nil
		}
		res, err := run.Run(ctx, markerPruneCypher, map[string]any{"retention_ms": retentionMs, "limit": limit})
		if err != nil {
			return false, err
		}
		rec, err := res.Single(ctx)
		if err != nil {
			return false, err
		}
		n, ok := rec.Values[0].(int64)
		if !ok {
			return false, fmt.Errorf("neo4j: the deleted count is %T, want an integer", rec.Values[0])
		}
		if err := unlockExportGate(ctx, run); err != nil {
			return false, err
		}
		deleted = n
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}
