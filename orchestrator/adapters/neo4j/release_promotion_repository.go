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
// :Meta singleton, short-circuit on a release_id match, otherwise truncate
// :Table + :DEPENDS_ON, recreate from the payload, and MERGE :Meta.
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

// PromoteRelease executes the atomic topology swap described in spec §6.4 as
// a single explicit transaction:
//
//  1. Read :Meta {key:'current_release'} — if release_id already matches,
//     commit empty and return (false, nil) for idempotent redelivery.
//  2. MATCH (n:Table) DETACH DELETE n — truncate all table nodes.
//  3. UNWIND $topology — create :Table nodes keyed on unique_id.
//  4. UNWIND upstream_unique_ids — create :DEPENDS_ON edges between :Table
//     nodes present in the candidate set. References to unique_ids outside
//     the set are silently skipped (MATCH finds no target and the UNWIND
//     iteration produces no row), matching dbt's compile model where only
//     intra-topology dependencies are resolved.
//  5. MERGE :Meta singleton — set release_id and updated_at.
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

	// Step B — Truncate: remove all :Table nodes and their relationships.
	truncRes, err := tx.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	if err != nil {
		return false, fmt.Errorf("truncate :Table: %w", err)
	}
	if _, err := truncRes.Consume(ctx); err != nil {
		return false, fmt.Errorf("consume truncate result: %w", err)
	}

	// Build the topology parameter list shared by steps C and D. The
	// upstream_unique_ids slice is included so that Step D can resolve edges
	// without a second parameter binding.
	payload := make([]map[string]interface{}, 0, len(nodes))
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
			"image_tag":           n.ImageTag,
			"schedule":            n.Schedule,
			"upstream_unique_ids": upstreams,
		})
	}

	// Step C — Create :Table nodes (skip if no nodes to avoid an empty UNWIND).
	if len(nodes) > 0 {
		createRes, err := tx.Run(ctx, `
			UNWIND $topology AS t
			CREATE (:Table {
				unique_id:    t.unique_id,
				schema_name:  t.schema_name,
				table_name:   t.table_name,
				service_name: t.service_name,
				image_tag:    t.image_tag,
				schedule:     t.schedule,
				release_id:   $release_id
			})
		`, map[string]interface{}{
			"topology":   payload,
			"release_id": releaseID,
		})
		if err != nil {
			return false, fmt.Errorf("create :Table nodes: %w", err)
		}
		if _, err := createRes.Consume(ctx); err != nil {
			return false, fmt.Errorf("consume :Table create result: %w", err)
		}

		// Step D — Create :DEPENDS_ON edges between :Table nodes that are
		// present in the candidate set. The MATCH silently skips upstream
		// unique_ids that do not correspond to a :Table node created in Step C
		// (e.g. cross-service dependencies outside this topology slice).
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
