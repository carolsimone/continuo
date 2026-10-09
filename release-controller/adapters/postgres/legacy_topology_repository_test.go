//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyTopologyRepository_ListsInlineTopologiesWithoutAReference(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	runs := postgres.NewRunRepository(db, nil)
	for i, id := range []string{"rInline", "rMoved", "rNull", "rNone"} {
		require.NoError(t, runs.Save(ctx, pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef",
			release.ManifestKindDbt, time.Unix(int64(100+i), 0).UTC())))
	}
	_, err := db.Exec(`UPDATE release_pipeline_runs
		SET candidate_topology = '[{"unique_id":"a","node_type":"dbt-model","candidate_artifact_uri":"s3://b/candidate-sql/rInline/candidate_a.sql"}]'::jsonb
		WHERE run_id IN ('rInline', 'rMoved')`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE release_pipeline_runs SET candidate_topology_uri = 's3://b/x' WHERE run_id = 'rMoved'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE release_pipeline_runs SET candidate_topology = 'null'::jsonb WHERE run_id = 'rNull'`)
	require.NoError(t, err)

	repo := postgres.NewLegacyTopologyRepository(db)
	legacy, err := repo.ListRunsWithLegacyTopology(ctx)
	require.NoError(t, err)
	require.Len(t, legacy, 1, "only an inline topology without a reference is legacy")
	assert.Equal(t, "rInline", legacy[0].RunID)
	assert.Equal(t, release.Topology{{UniqueID: "a", NodeType: "dbt-model"}}, legacy[0].Topology)

	ref := release.TopologyRef{URI: "s3://b/tenants/default/topologies/rInline/topology.json.gz", SHA256: "ab", NodeCount: 1}
	require.NoError(t, repo.SetRunTopologyRef(ctx, "rInline", ref))
	legacy, err = repo.ListRunsWithLegacyTopology(ctx)
	require.NoError(t, err)
	assert.Empty(t, legacy)
	got, err := runs.Get(ctx, "rInline")
	require.NoError(t, err)
	assert.Equal(t, ref, got.CandidateTopologyRef())
}

func TestLegacyTopologyRepository_ListsRunsStillParsingSinceTheUpgrade(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	runs := postgres.NewRunRepository(db, nil)
	at := time.Unix(100, 0).UTC()
	for _, id := range []string{"rParsing", "rMovedOn", "rUnmarked"} {
		r := pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, at)
		require.NoError(t, r.TransitionToParsing(at))
		if id == "rMovedOn" {
			require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, at))
		}
		require.NoError(t, runs.Save(ctx, r))
		at = at.Add(time.Second)
	}
	_, err := db.Exec(`UPDATE release_pipeline_runs SET parsing_at_upgrade = true WHERE run_id IN ('rParsing', 'rMovedOn')`)
	require.NoError(t, err)

	repo := postgres.NewLegacyTopologyRepository(db)
	ids, err := repo.ListParsingAtUpgrade(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"rParsing"}, ids)

	require.NoError(t, repo.ClearParsingAtUpgrade(ctx, "rParsing"))
	ids, err = repo.ListParsingAtUpgrade(ctx)
	require.NoError(t, err)
	assert.Empty(t, ids)
}

func TestLegacyTopologyRepository_GetLegacyCurrentProd(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewLegacyTopologyRepository(db)
	ctx := context.Background()

	got, err := repo.GetLegacyCurrentProd(ctx)
	require.NoError(t, err)
	assert.Nil(t, got, "no current_prod row: nothing to backfill")

	_, err = db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'rLegacy', '[{"unique_id":"a","service_name":"svc","node_type":"dbt-model","upstream_unique_ids":[]}]'::jsonb, now())`)
	require.NoError(t, err)
	got, err = repo.GetLegacyCurrentProd(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rLegacy", got.ReleaseID)
	require.Len(t, got.Topology, 1)
	assert.Equal(t, "a", got.Topology[0].UniqueID)
	assert.Equal(t, "svc", got.Topology[0].ServiceName)

	_, err = db.Exec(`UPDATE current_prod SET topology_uri = 's3://b/k', topology_sha256 = 'f00d', node_count = 1 WHERE id = 1`)
	require.NoError(t, err)
	got, err = repo.GetLegacyCurrentProd(ctx)
	require.NoError(t, err)
	assert.Nil(t, got, "a current_prod that references an artifact needs no backfill")
}

// A row naming no release (an e2e fixture resets current_prod that way) has
// nothing to write an artifact for.
func TestLegacyTopologyRepository_GetLegacyCurrentProd_IgnoresAnUnnamedRelease(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, '', '[{"unique_id":"a"}]'::jsonb, now())`)
	require.NoError(t, err)
	got, err := postgres.NewLegacyTopologyRepository(db).GetLegacyCurrentProd(context.Background())
	require.NoError(t, err)
	assert.Nil(t, got)
}
