package handlers

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReleaseNotices_ARebaselineGetsNoNoticeAndNoPreflight(t *testing.T) {
	yes := true
	prodSeed := release.Node{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed",
		FullRefresh: &yes, ContentHash: "sha256:9c2b", SeedColumnsHash: "sha256:e5a0"}
	candSeed := prodSeed
	candSeed.ContentHash, candSeed.SeedColumnsHash = "sha256:71fe", "sha256:0d3c"
	view := release.Node{UniqueID: "analytics.fx_rates_current", NodeType: "dbt-model", Materialized: "view",
		UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}}
	candidate, prod := release.Topology{candSeed, view}, release.Topology{prodSeed}
	d := &Deps{Logger: slog.Default(), Family: warehouse.SchemaBound, Engine: "postgres"}

	rebaseline := pipeline.NewCandidate("rel-r", "finance", "tag", true, true, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	notices, preflights := releaseNotices(d, rebaseline, candidate, prod)
	require.NotNil(t, notices)
	require.NotNil(t, preflights)
	assert.Empty(t, notices)
	assert.Empty(t, preflights)

	bootstrap := pipeline.NewCandidate("rel-b", "finance", "tag", true, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(0, 0))
	notices, preflights = releaseNotices(d, bootstrap, candidate, prod)
	require.Len(t, notices, 1)
	assert.True(t, notices[0].Unvalidated)
	require.Len(t, preflights, 1)
	assert.Equal(t, []string{"analytics.fx_rates_current"}, preflights["analytics.seed_fx_rates_eur"].Allowed)
}

func TestReleaseNotices_AVerificationRunGetsNoNoticeAndNoPreflight(t *testing.T) {
	yes := true
	prodSeed := release.Node{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed",
		FullRefresh: &yes, ContentHash: "sha256:9c2b", SeedColumnsHash: "sha256:e5a0"}
	candSeed := prodSeed
	candSeed.ContentHash, candSeed.SeedColumnsHash = "sha256:71fe", "sha256:0d3c"
	view := release.Node{UniqueID: "analytics.fx_rates_current", NodeType: "dbt-model", Materialized: "view",
		UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}}
	var logBuf bytes.Buffer
	d := &Deps{Logger: slog.New(slog.NewTextHandler(&logBuf, nil)), Family: warehouse.SchemaBound, Engine: "postgres"}

	v := pipeline.NewVerification("verify-1", "finance", "tag", "rel-orig", 1, "", release.ManifestKindDbt, time.Unix(0, 0))
	notices, preflights := releaseNotices(d, v, release.Topology{candSeed, view}, release.Topology{prodSeed})
	require.NotNil(t, notices)
	require.NotNil(t, preflights)
	assert.Empty(t, notices)
	assert.Empty(t, preflights)
	assert.Contains(t, logBuf.String(), "verification run: no seed reload notices")
	assert.Contains(t, logBuf.String(), "release_id=verify-1")
}
