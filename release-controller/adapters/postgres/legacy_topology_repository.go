package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// LegacyTopologyRepository is the Postgres-backed
// repository.LegacyTopologyRepository over release_pipeline_runs.
type LegacyTopologyRepository struct{ q Queryer }

// NewLegacyTopologyRepository binds a LegacyTopologyRepository to q.
func NewLegacyTopologyRepository(q Queryer) *LegacyTopologyRepository {
	return &LegacyTopologyRepository{q: q}
}

var _ repository.LegacyTopologyRepository = (*LegacyTopologyRepository)(nil)

// ListRunsWithLegacyTopology selects rows whose candidate_topology holds a
// topology (not NULL, not the JSON null a run without one was saved with) and
// that name no artifact. A stored candidate_artifact_uri is ignored on decode.
func (r *LegacyTopologyRepository) ListRunsWithLegacyTopology(ctx context.Context) ([]repository.LegacyRunTopology, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT run_id, candidate_topology FROM release_pipeline_runs
		 WHERE candidate_topology IS NOT NULL
		   AND candidate_topology <> 'null'::jsonb
		   AND candidate_topology_uri IS NULL
		 ORDER BY created_at, run_id`)
	if err != nil {
		return nil, fmt.Errorf("select runs with an inline topology: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []repository.LegacyRunTopology
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("scan run with an inline topology: %w", err)
		}
		var dto serialization.TopologyDTO
		if err := json.Unmarshal(raw, &dto); err != nil {
			return nil, fmt.Errorf("decode the inline candidate_topology of %s: %w", id, err)
		}
		out = append(out, repository.LegacyRunTopology{RunID: id, Topology: dto.ToDomain()})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs with an inline topology: %w", err)
	}
	return out, nil
}

// SetRunTopologyRef writes ref's three columns on runID.
func (r *LegacyTopologyRepository) SetRunTopologyRef(ctx context.Context, runID string, ref release.TopologyRef) error {
	if _, err := r.q.ExecContext(ctx,
		`UPDATE release_pipeline_runs
		 SET candidate_topology_uri = $2, candidate_topology_sha256 = $3, candidate_node_count = $4
		 WHERE run_id = $1`,
		runID, ref.URI, ref.SHA256, ref.NodeCount); err != nil {
		return fmt.Errorf("record the topology artifact of %s: %w", runID, err)
	}
	return nil
}

// ListParsingAtUpgrade returns the marked runs still in parsing, oldest first.
func (r *LegacyTopologyRepository) ListParsingAtUpgrade(ctx context.Context) ([]string, error) {
	rows, err := r.q.QueryxContext(ctx,
		`SELECT run_id FROM release_pipeline_runs
		 WHERE parsing_at_upgrade AND status = 'parsing'
		 ORDER BY created_at, run_id`)
	if err != nil {
		return nil, fmt.Errorf("select runs parsing at upgrade: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan run parsing at upgrade: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs parsing at upgrade: %w", err)
	}
	return ids, nil
}

// ClearParsingAtUpgrade removes runID's mark.
func (r *LegacyTopologyRepository) ClearParsingAtUpgrade(ctx context.Context, runID string) error {
	if _, err := r.q.ExecContext(ctx,
		`UPDATE release_pipeline_runs SET parsing_at_upgrade = false WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("clear the upgrade mark of %s: %w", runID, err)
	}
	return nil
}
