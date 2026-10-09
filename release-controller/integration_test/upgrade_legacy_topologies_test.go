//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_UpgradeLegacyTopologies drives the one-time step against
// the Flyway-built schema: a run validating under the old code keeps going on
// an artifact written from its inline topology, a run left parsing is rejected
// with upgrade_interrupted, and a second start changes nothing.
func TestIntegration_UpgradeLegacyTopologies(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	runs := postgres.NewRunRepository(db, nil)

	validating := pipeline.NewCandidate("rOld", "service-1", "img", false, "acme/demo", "sha", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, validating.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, validating.TransitionToValidating(release.TopologyRef{}, []string{"a"}, time.Unix(102, 0).UTC()))
	require.NoError(t, runs.Save(ctx, validating))
	_, err := db.Exec(`UPDATE release_pipeline_runs SET candidate_topology = $1::jsonb WHERE run_id = 'rOld'`,
		`[{"unique_id":"a","service_name":"service-1","node_type":"dbt-model","image_tag":"img","upstream_unique_ids":[],"candidate_artifact_uri":"s3://test-bucket/candidate-sql/rOld/candidate_a.sql"}]`)
	require.NoError(t, err)

	parsing := pipeline.NewCandidate("rParsing", "service-1", "img", false, "acme/demo", "sha", release.ManifestKindDbt, time.Unix(110, 0).UTC())
	require.NoError(t, parsing.TransitionToParsing(time.Unix(111, 0).UTC()))
	require.NoError(t, runs.Save(ctx, parsing))
	_, err = db.Exec(`UPDATE release_pipeline_runs SET parsing_at_upgrade = true WHERE run_id = 'rParsing'`)
	require.NoError(t, err)

	require.NoError(t, handlers.UpgradeLegacyTopologies(ctx, deps))

	got, err := runs.Get(ctx, "rOld")
	require.NoError(t, err)
	ref := got.CandidateTopologyRef()
	require.False(t, ref.IsZero(), "the inline topology moved into an artifact")
	assert.Equal(t, 1, ref.NodeCount)
	topo, err := deps.Topologies.Load(ctx, ref)
	require.NoError(t, err)
	require.Len(t, topo, 1)
	assert.Equal(t, "img", topo[0].ImageTag)

	failed, err := runs.Get(ctx, "rParsing")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusRejected, failed.Status())
	assert.Equal(t, string(pkg_model.RejectReasonUpgradeInterrupted), failed.FailReason())
	var marked bool
	require.NoError(t, db.Get(&marked, `SELECT parsing_at_upgrade FROM release_pipeline_runs WHERE run_id = 'rParsing'`))
	assert.False(t, marked)

	countRejected := func() int {
		var n int
		require.NoError(t, db.Get(&n, `SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleaseRejectedV1))
		return n
	}
	assert.Equal(t, 1, countRejected())

	require.NoError(t, handlers.UpgradeLegacyTopologies(ctx, deps))
	assert.Equal(t, 1, countRejected(), "a second start changes nothing")
}

// The queue pump after the one-time step does not depend on the step having
// rejected a run: an upgrade that finds nothing to settle (the retry after a
// failed or interrupted pump) still promotes the oldest received run.
func TestIntegration_UpgradeLegacyTopologies_PumpsTheQueueWhenNoRunFailed(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	runs := postgres.NewRunRepository(db, nil)

	queued := pipeline.NewCandidate("rQueued", "service-1", "img", true, "acme/demo", "sha", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, runs.Save(ctx, queued))

	require.NoError(t, handlers.UpgradeLegacyTopologies(ctx, deps))

	got, err := runs.Get(ctx, "rQueued")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusCompiling, got.Status(), "the queued run was activated although no run was rejected")
	var rejected int
	require.NoError(t, db.Get(&rejected, `SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleaseRejectedV1))
	assert.Zero(t, rejected, "the upgrade step rejected nothing")
}
