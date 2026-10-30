package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/domain/topology"
)

// currentReleaseLockCypher locks the tenant's (:Meta {key:'current_release'})
// node, creating it on the tenant's first promotion, and reads the live
// promotion. SET takes the node's exclusive lock before RETURN reads it, so a
// concurrent promotion of the tenant blocks on this statement until the first
// one commits and then reads the seq it wrote.
const currentReleaseLockCypher = `
MERGE (m:Meta {tenant_id: $tenant_id, key: 'current_release'})
SET m._lock = true
RETURN m.release_id AS release_id, coalesce(m.promotion_seq, 0) AS promotion_seq`

// currentReleaseUnlockCypher removes the lock property of a promotion that
// leaves the topology alone, so the property never persists.
const currentReleaseUnlockCypher = `MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) REMOVE m._lock`

// retireDroppedNodesCypher retires every :Node of the tenant the release does
// not carry, a node with no unique_id included, and deletes the retired nodes'
// outgoing :DEPENDS_ON, which describe the live topology only. retired_at is
// stamped when the node goes from live to retired and kept on every later
// promotion that leaves it retired. Nothing else of a retired node changes: it
// keeps its properties, its :CURRENT version, its :FAILED rejections and the
// :EDITED edges of its fixes.
const retireDroppedNodesCypher = `
MATCH (n:Node {tenant_id: $tenant_id})
WHERE n.unique_id IS NULL OR NOT n.unique_id IN $unique_ids
SET n.retired_at = CASE WHEN coalesce(n.active, true) THEN $now ELSE n.retired_at END,
    n.active = false
WITH n
MATCH (n)-[d:DEPENDS_ON]->()
DELETE d`

// upsertReleaseNodesCypher MERGEs every node of the release by its identity
// and refreshes its properties, so a node that a release carries again comes
// back live. The graph names the node's schedule schedule_name, the artifact
// names it schedule.
const upsertReleaseNodesCypher = `
UNWIND $topology AS t
MERGE (n:Node {tenant_id: $tenant_id, unique_id: t.unique_id})
SET n.schema_name        = t.schema_name,
    n.table_name         = t.table_name,
    n.service_name       = t.service_name,
    n.node_type          = t.node_type,
    n.content_hash       = t.content_hash,
    n.test_count         = t.test_count,
    n.image_tag          = t.image_tag,
    n.secret_ref         = CASE WHEN t.secret_ref = '' THEN null ELSE t.secret_ref END,
    n.schedule_name      = t.schedule,
    n.original_file_path = t.original_file_path,
    n.release_id         = $release_id,
    n.active             = true,
    n.retired_at         = null`

// clearReleaseEdgesCypher deletes the outgoing :DEPENDS_ON of the release's
// nodes, so the next statement rebuilds them without keeping a dependency the
// release dropped.
const clearReleaseEdgesCypher = `
UNWIND $topology AS t
MATCH (n:Node {tenant_id: $tenant_id, unique_id: t.unique_id})-[d:DEPENDS_ON]->()
DELETE d`

// drawReleaseEdgesCypher draws each node's :DEPENDS_ON to its upstreams among
// the release's nodes. By this statement the release's nodes are exactly the
// tenant's live ones, so an upstream that is retired, or that the graph does
// not hold (a dependency outside this topology), draws no edge.
const drawReleaseEdgesCypher = `
UNWIND $topology AS t
UNWIND t.upstream_unique_ids AS up
MATCH (a:Node {tenant_id: $tenant_id, unique_id: t.unique_id}), (b:Node {tenant_id: $tenant_id, unique_id: up})
WHERE b.active = true
CREATE (a)-[:DEPENDS_ON]->(b)`

// recordPromotionCypher makes the promotion the tenant's live one and removes
// the lock property.
const recordPromotionCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'})
SET m.release_id    = $release_id,
    m.promotion_seq = $promotion_seq,
    m.updated_at    = $now
REMOVE m._lock`

// releasePromotionRepository is the topology swap of one tenant, run on the
// open transaction of a unit-of-work Write. The Write has already locked the
// tenant's export gate, so the swap's lock on current_release is the second
// lock its transaction takes.
type releasePromotionRepository struct {
	run    TenantRunner
	logger *slog.Logger
}

var _ repository.ReleasePromotionRepository = (*releasePromotionRepository)(nil)

// PromoteRelease swaps tenantID's live topology on the open transaction:
//
//  1. Lock the tenant's current_release node and read the live promotion.
//  2. Decide with topology.DecidePromotion. A redelivered or stale promotion
//     removes the lock property and writes nothing.
//  3. Retire every :Node of the tenant the release does not carry, and delete
//     the retired nodes' outgoing :DEPENDS_ON.
//  4. MERGE every node of the release by (tenant_id, unique_id), refresh its
//     properties and make it live.
//  5. Rebuild the :DEPENDS_ON among the release's nodes.
//  6. Record release_id, promotion_seq and updated_at on current_release and
//     remove the lock property.
//
// It refuses a tenantID other than the transaction's. Nothing is deleted: a
// retired node stays with its history.
func (r *releasePromotionRepository) PromoteRelease(
	ctx context.Context,
	tenantID string,
	releaseID string,
	promotionSeq int64,
	nodes []topology.ReleasePromotedTopologyNode,
	now time.Time,
) (topology.PromotionOutcome, error) {
	if err := r.run.requireTenant(tenantID); err != nil {
		return "", err
	}
	if releaseID == "" {
		return "", errors.New("neo4j: a promotion needs a release id")
	}

	liveReleaseID, liveSeq, err := r.lockCurrentRelease(ctx)
	if err != nil {
		return "", err
	}

	outcome := topology.DecidePromotion(liveSeq, liveReleaseID, promotionSeq, releaseID)
	if outcome != topology.PromotionApplied {
		if promotionSeq == liveSeq && releaseID != liveReleaseID {
			r.logger.Error("release.promoted: promotion seq is already live under another release; topology left unchanged",
				"tenant_id", tenantID, "release_id", releaseID, "promotion_seq", promotionSeq, "live_release_id", liveReleaseID)
		} else {
			r.logger.Info("release.promoted: topology left unchanged",
				"tenant_id", tenantID, "outcome", string(outcome),
				"release_id", releaseID, "promotion_seq", promotionSeq,
				"live_release_id", liveReleaseID, "live_promotion_seq", liveSeq)
		}
		if err := execCypher(ctx, r.run, currentReleaseUnlockCypher, nil); err != nil {
			return "", fmt.Errorf("unlock current release: %w", err)
		}
		return outcome, nil
	}

	// The Neo4j Go driver names a time.Time's zone by its Location().String(),
	// and Neo4j rejects time.Local's "Local", so every time is sent in UTC.
	topo, uniqueIDs := topologyParams(nodes)
	if err := execCypher(ctx, r.run, retireDroppedNodesCypher, map[string]any{
		"unique_ids": uniqueIDs,
		"now":        now.UTC(),
	}); err != nil {
		return "", fmt.Errorf("retire the nodes the release drops: %w", err)
	}
	if len(nodes) > 0 {
		if err := execCypher(ctx, r.run, upsertReleaseNodesCypher, map[string]any{
			"topology":   topo,
			"release_id": releaseID,
		}); err != nil {
			return "", fmt.Errorf("upsert the release's nodes: %w", err)
		}
		if err := execCypher(ctx, r.run, clearReleaseEdgesCypher, map[string]any{"topology": topo}); err != nil {
			return "", fmt.Errorf("clear the release's :DEPENDS_ON: %w", err)
		}
		if err := execCypher(ctx, r.run, drawReleaseEdgesCypher, map[string]any{"topology": topo}); err != nil {
			return "", fmt.Errorf("draw the release's :DEPENDS_ON: %w", err)
		}
	}
	if err := execCypher(ctx, r.run, recordPromotionCypher, map[string]any{
		"release_id":    releaseID,
		"promotion_seq": promotionSeq,
		"now":           now.UTC(),
	}); err != nil {
		return "", fmt.Errorf("record the promotion on current release: %w", err)
	}

	r.logger.Info("release.promoted: applied topology swap",
		"tenant_id", tenantID, "release_id", releaseID, "promotion_seq", promotionSeq, "node_count", len(nodes))
	return topology.PromotionApplied, nil
}

// lockCurrentRelease locks the tenant's current_release node and returns the
// live release and promotion seq; both are zero on the tenant's first
// promotion.
func (r *releasePromotionRepository) lockCurrentRelease(ctx context.Context) (string, int64, error) {
	res, err := r.run.Run(ctx, currentReleaseLockCypher, nil)
	if err != nil {
		return "", 0, fmt.Errorf("lock current release: %w", err)
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("read current release: %w", err)
	}
	var releaseID string
	if v, ok := rec.Get("release_id"); ok && v != nil {
		releaseID, _ = v.(string)
	}
	var seq int64
	if v, ok := rec.Get("promotion_seq"); ok && v != nil {
		seq, _ = v.(int64)
	}
	return releaseID, seq, nil
}

// topologyParams renders a release's nodes as the $topology parameter of the
// statements that write them as :Node, and lists their unique ids.
func topologyParams(nodes []topology.ReleasePromotedTopologyNode) ([]map[string]any, []string) {
	topo := make([]map[string]any, 0, len(nodes))
	uniqueIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		upstreams := n.UpstreamUniqueIDs
		if upstreams == nil {
			upstreams = []string{}
		}
		topo = append(topo, map[string]any{
			"unique_id":           n.UniqueID,
			"schema_name":         n.SchemaName,
			"table_name":          n.TableName,
			"service_name":        n.ServiceName,
			"node_type":           n.NodeType,
			"content_hash":        n.ContentHash,
			"test_count":          n.TestCount,
			"image_tag":           n.ImageTag,
			"secret_ref":          n.SecretRef,
			"schedule":            n.Schedule,
			"upstream_unique_ids": upstreams,
			"original_file_path":  n.OriginalFilePath,
		})
		uniqueIDs = append(uniqueIDs, n.UniqueID)
	}
	return topo, uniqueIDs
}
