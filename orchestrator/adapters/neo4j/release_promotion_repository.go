package neo4jinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ReleasePromotionRepository implements repository.ReleasePromotionRepository
// against Neo4j. The swap runs in a single explicit transaction: lock the :Meta
// singleton, compare the promotion seq with the live one, and only for a newer
// promotion apply the retire-then-orphan-cleanup pattern and record the
// promotion on :Meta and :TopologyRoot.
type ReleasePromotionRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ repository.ReleasePromotionRepository = (*ReleasePromotionRepository)(nil)

// NewReleasePromotionRepository constructs a ReleasePromotionRepository backed
// by the given Neo4j client.
func NewReleasePromotionRepository(client Neo4jClient, logger *slog.Logger) *ReleasePromotionRepository {
	return &ReleasePromotionRepository{client: client, logger: logger}
}

// PromoteRelease executes the topology swap as a single explicit transaction:
//
//  1. Write-lock :Meta {key:'current_release'}, then read its release_id and
//     promotion_seq. Setting a property takes the node's exclusive lock before
//     the RETURN reads it, so a concurrent promotion blocks on this statement
//     until the first transaction commits and then reads the seq it wrote.
//  2. Decide with topology.DecidePromotion. A redelivered or stale promotion
//     releases the lock property and commits without touching the graph.
//  3. Retire :Table nodes NOT in the new topology by setting active=false,
//     retired_at — preserving :Run-[:EXECUTES]->:Table edges so run history
//     is not destroyed.
//  4. MERGE :Table nodes from the new topology by unique_id and refresh all
//     properties, re-activating nodes that may have been previously retired.
//  5. Clear outgoing :DEPENDS_ON edges on upserted nodes and rebuild them
//     between nodes in the new topology; references to unique_ids outside the
//     set are silently skipped.
//  6. Delete :Table nodes that are inactive AND have no incoming
//     :Run-[:EXECUTES] reference.
//  7. Record release_id, promotion_seq and updated_at on :Meta and release the
//     lock property; record service_metadata and promotion_seq on
//     :TopologyRoot, where new runs read them.
func (r *ReleasePromotionRepository) PromoteRelease(
	ctx context.Context,
	releaseID string,
	promotionSeq int64,
	nodes []topology.ReleasePromotedTopologyNode,
	serviceMetadata map[string]map[string]string,
	now time.Time,
) (topology.PromotionOutcome, error) {
	if releaseID == "" {
		return "", fmt.Errorf("release id is empty")
	}
	if serviceMetadata == nil {
		serviceMetadata = map[string]map[string]string{}
	}
	metadataJSON, err := json.Marshal(serviceMetadata)
	if err != nil {
		return "", fmt.Errorf("marshal service_metadata: %w", err)
	}

	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return "", fmt.Errorf("begin release-promotion tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step A — lock :Meta, then read the live promotion. The _lock property is
	// removed again before this transaction commits, so it never persists.
	lockRes, err := tx.Run(ctx, `
		MERGE (m:Meta {key: 'current_release'})
		SET m._lock = true
		RETURN m.release_id AS release_id, COALESCE(m.promotion_seq, 0) AS promotion_seq
	`, nil)
	if err != nil {
		return "", fmt.Errorf("lock current release meta: %w", err)
	}
	var liveReleaseID string
	var liveSeq int64
	if lockRes.Next(ctx) {
		rec := lockRes.Record()
		if v, ok := rec.Get("release_id"); ok && v != nil {
			liveReleaseID, _ = v.(string)
		}
		if v, ok := rec.Get("promotion_seq"); ok && v != nil {
			liveSeq, _ = v.(int64)
		}
	}
	if err := lockRes.Err(); err != nil {
		return "", fmt.Errorf("read current release meta: %w", err)
	}

	outcome := topology.DecidePromotion(liveSeq, liveReleaseID, promotionSeq, releaseID)
	if outcome != topology.PromotionApplied {
		if promotionSeq == liveSeq && releaseID != liveReleaseID {
			r.logger.Error("release.promoted: promotion seq is already live under another release — topology left unchanged",
				"release_id", releaseID, "promotion_seq", promotionSeq, "live_release_id", liveReleaseID)
		} else {
			r.logger.Info("release.promoted: topology left unchanged",
				"outcome", string(outcome),
				"release_id", releaseID, "promotion_seq", promotionSeq,
				"live_release_id", liveReleaseID, "live_promotion_seq", liveSeq)
		}
		unlockRes, err := tx.Run(ctx, `MATCH (m:Meta {key: 'current_release'}) REMOVE m._lock`, nil)
		if err != nil {
			return "", fmt.Errorf("release :Meta lock: %w", err)
		}
		if _, err := unlockRes.Consume(ctx); err != nil {
			return "", fmt.Errorf("consume :Meta unlock result: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("commit unchanged release-promotion tx: %w", err)
		}
		return outcome, nil
	}

	// Build the topology parameter list shared by steps C–D. The
	// upstream_unique_ids slice is included so that Step D can resolve edges
	// without a second parameter binding.
	payload := make([]map[string]interface{}, 0, len(nodes))
	newUniqueIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		upstreams := n.UpstreamUniqueIDs
		if upstreams == nil {
			upstreams = []string{}
		}
		payload = append(payload, map[string]interface{}{
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
		newUniqueIDs = append(newUniqueIDs, n.UniqueID)
	}

	// Step B — Retire :Table nodes that are not in the new topology. Setting
	// active=false and retired_at preserves the nodes (and their incoming
	// :Run-[:EXECUTES] edges) so that run history is not destroyed. The
	// `unique_id IS NULL` clause also retires any :Table node that carries no
	// unique_id; without it such a node (whose `unique_id IN $list` evaluates to
	// NULL, never matching the negation) would linger active beside the
	// unique_id-keyed nodes this release creates.
	retireRes, err := tx.Run(ctx, `
		MATCH (t:Table)
		WHERE t.unique_id IS NULL OR NOT t.unique_id IN $new_unique_ids
		SET t.retired_at = CASE WHEN COALESCE(t.active, true) THEN $now ELSE t.retired_at END,
		    t.active = false
	`, map[string]interface{}{
		"new_unique_ids": newUniqueIDs,
		"now":            now.UTC(),
	})
	if err != nil {
		return "", fmt.Errorf("retire stale :Table nodes: %w", err)
	}
	if _, err := retireRes.Consume(ctx); err != nil {
		return "", fmt.Errorf("consume retire result: %w", err)
	}

	// Steps C, C.5, D — Only execute if the new topology is non-empty.
	if len(nodes) > 0 {
		// Step C — MERGE :Table nodes from the new topology, keyed on unique_id.
		// Properties are set (not created) so that nodes surviving across releases
		// are refreshed in place. active=true and retired_at=NULL re-activate any
		// previously retired node that is back in the topology. The Neo4j property
		// name is schedule_name (matching every existing reader query) while the
		// node field stays Schedule (matching the topology artifact's shape).
		upsertRes, err := tx.Run(ctx, `
			UNWIND $topology AS t
			MERGE (existing:Table {unique_id: t.unique_id})
			SET existing.schema_name  = t.schema_name,
			    existing.table_name   = t.table_name,
			    existing.service_name = t.service_name,
			    existing.node_type    = t.node_type,
			    existing.content_hash = t.content_hash,
			    existing.test_count   = t.test_count,
			    existing.image_tag    = t.image_tag,
			    existing.secret_ref   = CASE WHEN t.secret_ref = '' THEN null ELSE t.secret_ref END,
			    existing.schedule_name = t.schedule,
			    existing.original_file_path = t.original_file_path,
			    existing.release_id   = $release_id,
			    existing.active       = true,
			    existing.retired_at   = null
		`, map[string]interface{}{
			"topology":   payload,
			"release_id": releaseID,
		})
		if err != nil {
			return "", fmt.Errorf("upsert :Table nodes: %w", err)
		}
		if _, err := upsertRes.Consume(ctx); err != nil {
			return "", fmt.Errorf("consume :Table upsert result: %w", err)
		}

		// Step C.5 — Clear existing :DEPENDS_ON edges on the upserted nodes so
		// they can be rebuilt from scratch without leaving stale edges when a
		// dependency is removed between releases.
		clearEdgeRes, err := tx.Run(ctx, `
			UNWIND $topology AS t
			MATCH (a:Table {unique_id: t.unique_id})-[r:DEPENDS_ON]->()
			DELETE r
		`, map[string]interface{}{
			"topology": payload,
		})
		if err != nil {
			return "", fmt.Errorf("clear :DEPENDS_ON edges: %w", err)
		}
		if _, err := clearEdgeRes.Consume(ctx); err != nil {
			return "", fmt.Errorf("consume clear edges result: %w", err)
		}

		// Step D — Rebuild :DEPENDS_ON edges between :Table nodes present in the
		// new topology. The MATCH silently skips upstream unique_ids that do not
		// correspond to a :Table node in the candidate set (e.g. cross-service
		// dependencies outside this topology slice).
		edgeRes, err := tx.Run(ctx, `
			UNWIND $topology AS t
			UNWIND t.upstream_unique_ids AS up
			MATCH (a:Table {unique_id: t.unique_id}), (b:Table {unique_id: up})
			CREATE (a)-[:DEPENDS_ON]->(b)
		`, map[string]interface{}{
			"topology": payload,
		})
		if err != nil {
			return "", fmt.Errorf("create :DEPENDS_ON edges: %w", err)
		}
		if _, err := edgeRes.Consume(ctx); err != nil {
			return "", fmt.Errorf("consume :DEPENDS_ON edge result: %w", err)
		}
	}

	// Step D.5 — Delete inactive :Table nodes that have no incoming
	// :Run-[:EXECUTES] reference. Nodes that are still referenced by a Run
	// remain in the graph (retired) so that run history stays intact.
	orphanRes, err := tx.Run(ctx, `
		MATCH (t:Table)
		WHERE COALESCE(t.active, true) = false
		  AND NOT EXISTS { MATCH (:Run)-[:EXECUTES]->(t) }
		DETACH DELETE t
	`, nil)
	if err != nil {
		return "", fmt.Errorf("delete inactive orphan :Table nodes: %w", err)
	}
	if _, err := orphanRes.Consume(ctx); err != nil {
		return "", fmt.Errorf("consume orphan delete result: %w", err)
	}

	// Step E — Record the promotion on :Meta and release the lock. Neo4j's Go
	// driver serialises time.Time using its Location().String() as a timezone
	// identifier; time.Local serialises to "Local", which Neo4j rejects
	// ("Illegal zone identifier"). Convert to UTC so the identifier is a valid
	// IANA zone regardless of the caller's locale.
	metaRes, err := tx.Run(ctx, `
		MATCH (m:Meta {key: 'current_release'})
		SET m.release_id    = $release_id,
		    m.promotion_seq = $promotion_seq,
		    m.updated_at    = $now
		REMOVE m._lock
	`, map[string]interface{}{
		"release_id":    releaseID,
		"promotion_seq": promotionSeq,
		"now":           now.UTC(),
	})
	if err != nil {
		return "", fmt.Errorf("record promotion on :Meta: %w", err)
	}
	if _, err := metaRes.Consume(ctx); err != nil {
		return "", fmt.Errorf("consume :Meta update result: %w", err)
	}

	// Step F — Record the live promotion on :TopologyRoot, where the snapshot
	// writer reads the seq and service metadata a new run is stamped with.
	rootRes, err := tx.Run(ctx, `
		MERGE (root:TopologyRoot {id: 'singleton'})
		SET root.service_metadata = $service_metadata,
		    root.promotion_seq    = $promotion_seq,
		    root.updated_at       = datetime()
		REMOVE root.topology_generation
	`, map[string]interface{}{
		"service_metadata": string(metadataJSON),
		"promotion_seq":    promotionSeq,
	})
	if err != nil {
		return "", fmt.Errorf("record promotion on :TopologyRoot: %w", err)
	}
	if _, err := rootRes.Consume(ctx); err != nil {
		return "", fmt.Errorf("consume :TopologyRoot update result: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit release-promotion tx: %w", err)
	}

	r.logger.Info("release.promoted: applied topology swap",
		"release_id", releaseID,
		"promotion_seq", promotionSeq,
		"node_count", len(nodes),
	)
	return topology.PromotionApplied, nil
}

// StillDesiredSeeds matches a late promotion's changed seeds against the live
// topology. A seed qualifies only when the live :Table with its unique_id is an
// active dbt-seed with the same, non-empty content_hash: the live release then
// still wants exactly that data, so building it cannot overwrite newer content.
// The returned nodes carry the live identity and image_tag.
func (r *ReleasePromotionRepository) StillDesiredSeeds(
	ctx context.Context,
	seeds []topology.ReleasePromotedTopologyNode,
) ([]topology.ReleasePromotedTopologyNode, error) {
	if len(seeds) == 0 {
		return nil, nil
	}
	params := make([]map[string]any, 0, len(seeds))
	for _, s := range seeds {
		params = append(params, map[string]any{"unique_id": s.UniqueID, "content_hash": s.ContentHash})
	}

	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	res, err := session.Run(ctx, `
		UNWIND $seeds AS s
		MATCH (t:Table {unique_id: s.unique_id})
		WHERE COALESCE(t.active, true)
		  AND t.node_type = 'dbt-seed'
		  AND s.content_hash <> ''
		  AND t.content_hash = s.content_hash
		RETURN t.unique_id    AS unique_id,
		       t.schema_name  AS schema_name,
		       t.table_name   AS table_name,
		       t.service_name AS service_name,
		       t.node_type    AS node_type,
		       t.content_hash AS content_hash,
		       t.image_tag    AS image_tag,
		       t.schedule_name AS schedule_name
		ORDER BY unique_id
	`, map[string]any{"seeds": params})
	if err != nil {
		return nil, fmt.Errorf("match late seeds against the live topology: %w", err)
	}
	var out []topology.ReleasePromotedTopologyNode
	for res.Next(ctx) {
		rec := res.Record()
		out = append(out, topology.ReleasePromotedTopologyNode{
			UniqueID:    safeString(recordValue(rec, "unique_id")),
			SchemaName:  safeString(recordValue(rec, "schema_name")),
			TableName:   safeString(recordValue(rec, "table_name")),
			ServiceName: safeString(recordValue(rec, "service_name")),
			NodeType:    safeString(recordValue(rec, "node_type")),
			ContentHash: safeString(recordValue(rec, "content_hash")),
			ImageTag:    safeString(recordValue(rec, "image_tag")),
			Schedule:    safeString(recordValue(rec, "schedule_name")),
		})
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("iterate late seed matches: %w", err)
	}
	return out, nil
}
