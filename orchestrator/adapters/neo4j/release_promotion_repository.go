package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ReleasePromotionRepository implements repository.ReleasePromotionRepository
// against Neo4j. The swap runs in a single explicit transaction: read the
// :Meta singleton, short-circuit on a release_id match, otherwise apply a
// retire-then-orphan-cleanup pattern, and MERGE :Meta.
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

// PromoteRelease executes the atomic topology swap as a single explicit
// transaction:
//
//  1. Read :Meta {key:'current_release'} — if release_id already matches,
//     commit empty and return (false, nil) for idempotent redelivery.
//  2. Retire :Table nodes NOT in the new topology by setting active=false,
//     retired_at — preserving :Run-[:EXECUTES]->:Table edges so run history
//     is not destroyed.
//  3. MERGE :Table nodes from the new topology by unique_id and refresh all
//     properties, re-activating nodes that may have been previously retired.
//  4. Clear outgoing :DEPENDS_ON edges on upserted nodes so they can be
//     rebuilt from scratch.
//  5. Rebuild :DEPENDS_ON edges between nodes in the new topology. References
//     to unique_ids outside the set are silently skipped.
//  6. Delete :Table nodes that are inactive AND have no incoming
//     :Run-[:EXECUTES] reference. Inactive nodes still referenced by a run
//     remain (retired) so run history is preserved.
//  7. MERGE :Meta singleton — set release_id and updated_at.
func (r *ReleasePromotionRepository) PromoteRelease(
	ctx context.Context,
	releaseID string,
	nodes []topology.ReleasePromotedTopologyNode,
	now time.Time,
) (bool, error) {
	if releaseID == "" {
		return false, fmt.Errorf("release id is empty")
	}

	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return false, fmt.Errorf("begin release-promotion tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step A — Read current release_id inside the transaction so that the
	// idempotency check is part of the same serializable unit of work.
	readRes, err := tx.Run(ctx, `
		OPTIONAL MATCH (m:Meta {key: 'current_release'})
		RETURN m.release_id AS current_release_id
	`, nil)
	if err != nil {
		return false, fmt.Errorf("read current release meta: %w", err)
	}
	var currentReleaseID string
	if readRes.Next(ctx) {
		if v, ok := readRes.Record().Get("current_release_id"); ok && v != nil {
			currentReleaseID, _ = v.(string)
		}
	}
	if err := readRes.Err(); err != nil {
		return false, fmt.Errorf("iterate current release meta result: %w", err)
	}

	// Idempotent no-op: commit the empty transaction and signal no change.
	if currentReleaseID == releaseID {
		r.logger.Info("release.promoted: current_release already matches — no-op",
			"release_id", releaseID,
		)
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit no-op release-promotion tx: %w", err)
		}
		return false, nil
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
			"image_tag":           n.ImageTag,
			"schedule":            n.Schedule,
			"upstream_unique_ids": upstreams,
		})
		newUniqueIDs = append(newUniqueIDs, n.UniqueID)
	}

	// Step B — Retire :Table nodes that are not in the new topology. Setting
	// active=false and retired_at preserves the nodes (and their incoming
	// :Run-[:EXECUTES] edges) so that run history is not destroyed.
	retireRes, err := tx.Run(ctx, `
		MATCH (t:Table)
		WHERE NOT t.unique_id IN $new_unique_ids
		SET t.active = false, t.retired_at = $now
	`, map[string]interface{}{
		"new_unique_ids": newUniqueIDs,
		"now":            now.UTC(),
	})
	if err != nil {
		return false, fmt.Errorf("retire stale :Table nodes: %w", err)
	}
	if _, err := retireRes.Consume(ctx); err != nil {
		return false, fmt.Errorf("consume retire result: %w", err)
	}

	// Steps C, C.5, D — Only execute if the new topology is non-empty.
	if len(nodes) > 0 {
		// Step C — MERGE :Table nodes from the new topology, keyed on unique_id.
		// Properties are set (not created) so that nodes surviving across releases
		// are refreshed in place. active=true and retired_at=NULL re-activate any
		// previously retired node that is back in the topology. The Neo4j property
		// name is schedule_name (matching every existing reader query) while the
		// Go wire DTO field stays schedule (matching release.promoted:v1's shape).
		upsertRes, err := tx.Run(ctx, `
			UNWIND $topology AS t
			MERGE (existing:Table {unique_id: t.unique_id})
			SET existing.schema_name  = t.schema_name,
			    existing.table_name   = t.table_name,
			    existing.service_name = t.service_name,
			    existing.node_type    = t.node_type,
			    existing.image_tag    = t.image_tag,
			    existing.schedule_name = t.schedule,
			    existing.release_id   = $release_id,
			    existing.active       = true,
			    existing.retired_at   = null
		`, map[string]interface{}{
			"topology":   payload,
			"release_id": releaseID,
		})
		if err != nil {
			return false, fmt.Errorf("upsert :Table nodes: %w", err)
		}
		if _, err := upsertRes.Consume(ctx); err != nil {
			return false, fmt.Errorf("consume :Table upsert result: %w", err)
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
			return false, fmt.Errorf("clear :DEPENDS_ON edges: %w", err)
		}
		if _, err := clearEdgeRes.Consume(ctx); err != nil {
			return false, fmt.Errorf("consume clear edges result: %w", err)
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
			return false, fmt.Errorf("create :DEPENDS_ON edges: %w", err)
		}
		if _, err := edgeRes.Consume(ctx); err != nil {
			return false, fmt.Errorf("consume :DEPENDS_ON edge result: %w", err)
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
		return false, fmt.Errorf("delete inactive orphan :Table nodes: %w", err)
	}
	if _, err := orphanRes.Consume(ctx); err != nil {
		return false, fmt.Errorf("consume orphan delete result: %w", err)
	}

	// Step E — MERGE :Meta singleton and stamp new release_id + timestamp.
	// Neo4j's Go driver serialises time.Time using its Location().String()
	// as a timezone identifier; time.Local serialises to "Local", which
	// Neo4j rejects ("Illegal zone identifier"). Convert to UTC so the
	// identifier is a valid IANA zone regardless of the caller's locale.
	metaRes, err := tx.Run(ctx, `
		MERGE (m:Meta {key: 'current_release'})
		SET m.release_id = $release_id,
		    m.updated_at = $now
	`, map[string]interface{}{
		"release_id": releaseID,
		"now":        now.UTC(),
	})
	if err != nil {
		return false, fmt.Errorf("merge :Meta singleton: %w", err)
	}
	if _, err := metaRes.Consume(ctx); err != nil {
		return false, fmt.Errorf("consume :Meta merge result: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit release-promotion tx: %w", err)
	}

	r.logger.Info("release.promoted: applied topology swap",
		"release_id", releaseID,
		"node_count", len(nodes),
	)
	return true, nil
}
