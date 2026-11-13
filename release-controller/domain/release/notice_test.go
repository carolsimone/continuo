package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rebuildIDs(n release.Notice) []string {
	ids := make([]string, 0, len(n.Rebuilds))
	for _, r := range n.Rebuilds {
		ids = append(ids, r.Key)
	}
	return ids
}

// fxSeeds returns the design's seed as production holds it and as the
// candidate carries it, with the column source added.
func fxSeeds() (prodSeed, candSeed release.Node) {
	yes := true
	prodSeed = release.Node{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed", FullRefresh: &yes,
		ContentHash: "sha256:9c2b", SeedColumnsHash: "sha256:e5a0", Schedule: "seed",
		SeedColumns: []release.SeedColumn{{Name: "currency"}, {Name: "rate_date"}, {Name: "rate_to_eur"}}}
	candSeed = prodSeed
	candSeed.ContentHash, candSeed.SeedColumnsHash = "sha256:71fe", "sha256:0d3c"
	candSeed.SeedColumns = append(append([]release.SeedColumn{}, prodSeed.SeedColumns...), release.SeedColumn{Name: "source"})
	return prodSeed, candSeed
}

// fxViews returns the design's nodes below the seed: two views and a table.
func fxViews() release.Topology {
	return release.Topology{
		{UniqueID: "analytics.fx_rates_current", NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}},
		{UniqueID: "analytics.fx_rates_latest_eur", NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", UpstreamUniqueIDs: []string{"analytics.fx_rates_current"}},
		{UniqueID: "analytics.fx_transactions_eur", NodeType: "dbt-model", Materialized: "table", Schedule: "daily", UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}},
	}
}

func TestBuildNotices_TheDesignsExample(t *testing.T) {
	yes := true
	prodSeed := release.Node{UniqueID: "analytics.seed_fx_rates_eur", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed", FullRefresh: &yes,
		ContentHash: "sha256:9c2b", SeedColumnsHash: "sha256:e5a0", Schedule: "seed",
		SeedColumns: []release.SeedColumn{{Name: "currency"}, {Name: "rate_date"}, {Name: "rate_to_eur"}}}
	candSeed := prodSeed
	candSeed.ContentHash, candSeed.SeedColumnsHash = "sha256:71fe", "sha256:0d3c"
	candSeed.SeedColumns = append(append([]release.SeedColumn{}, prodSeed.SeedColumns...), release.SeedColumn{Name: "source"})
	views := release.Topology{
		{UniqueID: "analytics.fx_rates_current", NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}},
		{UniqueID: "analytics.fx_rates_latest_eur", NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", UpstreamUniqueIDs: []string{"analytics.fx_rates_current"}},
		{UniqueID: "analytics.fx_transactions_eur", NodeType: "dbt-model", Materialized: "table", Schedule: "daily", UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}},
	}
	notices, preflights := release.BuildNotices(append(release.Topology{candSeed}, views...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 1)
	n := notices[0]
	assert.Equal(t, release.ColumnChanges{Added: []string{"source"}, Removed: []string{}, Retyped: []string{}}, n.Columns)
	assert.Equal(t, seedreload.Load{Operation: model.OperationRun, DropsTable: true, Reason: "columns changed; +full_refresh: true re-creates the table on every load"}, n.Load)
	assert.Equal(t, []string{"analytics.fx_rates_current", "analytics.fx_rates_latest_eur"}, rebuildIDs(n))
	assert.Equal(t, []seedreload.Boundary{{Key: "analytics.fx_transactions_eur", Materialized: "table", Schedule: "daily"}}, n.RefreshOnSchedule)
	assert.Equal(t, "reloads", n.Prediction)
	assert.False(t, n.Warehouse.Checked)
	assert.Equal(t, seedreload.PreflightReport, preflights["analytics.seed_fx_rates_eur"].Mode)

	n.ApplyFindings(seedreload.Findings{Checked: true, Completeness: "exact", Dependents: []seedreload.Dependent{
		{Schema: "analytics", Name: "fx_rates_current", Kind: "view", Owner: "dbt", Depth: 1, Allowed: true},
		{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1}}})
	assert.Equal(t, "stops", n.Prediction)
	assert.Equal(t, "unowned_dependents", n.StopReason)
	assert.Equal(t, []release.OutsideObject{{Object: "bi.fx_rates_report", Kind: "view", Owner: "bi"}}, n.Warehouse.OutsideContinuo)
}

func TestBuildNotices_TheNoticeNamesTheSeedAndItsChange(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	notices, preflights := release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 1)
	n := notices[0]
	assert.Equal(t, "seed_reload", n.Kind)
	assert.Equal(t, "analytics.seed_fx_rates_eur", n.NodeID)
	assert.Equal(t, "finance", n.Service)
	assert.Equal(t, warehouse.SchemaBound, n.EngineFamily)
	assert.Equal(t, seedreload.ChangeColumns, n.Change)
	assert.Empty(t, n.StopReason)
	assert.False(t, n.Unvalidated)
	p := preflights["analytics.seed_fx_rates_eur"]
	require.NotNil(t, p)
	assert.Equal(t, "analytics.seed_fx_rates_eur", p.Relation)
	assert.Equal(t, []string{"analytics.fx_rates_current", "analytics.fx_rates_latest_eur"}, p.Allowed)
}

func TestBuildNotices_SkipsNewUnchangedAndNonDroppingSeeds(t *testing.T) {
	unset := release.Node{UniqueID: "analytics.seed_unset", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed",
		ContentHash: "sha256:a1", SeedColumnsHash: "sha256:c1"}
	unsetCand := unset
	unsetCand.ContentHash = "sha256:a2"
	same := release.Node{UniqueID: "analytics.seed_same", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed",
		ContentHash: "sha256:b1", SeedColumnsHash: "sha256:c1"}
	brandNew := release.Node{UniqueID: "analytics.seed_new", ServiceName: "finance", NodeType: "dbt-seed", Materialized: "seed",
		ContentHash: "sha256:n1", SeedColumnsHash: "sha256:c1"}
	view := release.Node{UniqueID: "analytics.v", NodeType: "dbt-model", Materialized: "view",
		UpstreamUniqueIDs: []string{"analytics.seed_unset", "analytics.seed_same", "analytics.seed_new"}}

	notices, preflights := release.BuildNotices(release.Topology{unsetCand, same, brandNew, view}, release.Topology{unset, same}, warehouse.SchemaBound, "postgres", false)
	assert.Empty(t, notices)
	assert.Empty(t, preflights)
}

func TestBuildNotices_AFalseSeedWithNewColumnsIsRefused(t *testing.T) {
	no := false
	prodSeed, candSeed := fxSeeds()
	prodSeed.FullRefresh, candSeed.FullRefresh = &no, &no
	notices, preflights := release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 1)
	n := notices[0]
	assert.Equal(t, "refused", n.Prediction)
	assert.Equal(t, "seed_full_refresh_disabled", n.StopReason)
	assert.Equal(t, model.FailureReasonSeedFullRefreshDisabled, n.Load.Refusal)
	assert.Empty(t, n.Rebuilds)
	assert.NotContains(t, preflights, "analytics.seed_fx_rates_eur")

	// A refused load stays refused whatever the warehouse holds.
	n.ApplyFindings(seedreload.Findings{Checked: true, Completeness: "exact",
		Dependents: []seedreload.Dependent{{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1}}})
	assert.Equal(t, "refused", n.Prediction)
	assert.Equal(t, "seed_full_refresh_disabled", n.StopReason)
}

func TestBuildNotices_ABootstrapColumnChangeIsUnvalidated(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	notices, _ := release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", true)
	require.Len(t, notices, 1)
	n := notices[0]
	assert.True(t, n.Unvalidated)
	assert.Equal(t, "columns changed; +full_refresh: true re-creates the table on every load; unvalidated: its release skipped validation", n.Load.Reason)

	// A bootstrap whose seed changed only its rows is not unvalidated.
	candSeed.SeedColumnsHash = prodSeed.SeedColumnsHash
	candSeed.SeedColumns = prodSeed.SeedColumns
	notices, _ = release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", true)
	require.Len(t, notices, 1)
	assert.False(t, notices[0].Unvalidated)
	assert.Equal(t, seedreload.ChangeRows, notices[0].Change)
}

// A dbt-test node below a seed is validation-only: it is never a boundary that
// refreshes on its schedule.
func TestBuildNotices_LeavesTestsOutOfTheWalk(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	candidate := append(release.Topology{candSeed}, fxViews()...)
	candidate = append(candidate,
		release.Node{UniqueID: "test.not_null_seed_fx_rates_eur_currency", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"analytics.seed_fx_rates_eur"}},
		release.Node{UniqueID: "test.unique_fx_rates_current", NodeType: "dbt-test", UpstreamUniqueIDs: []string{"analytics.fx_rates_current"}},
	)
	notices, _ := release.BuildNotices(candidate, release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 1)
	assert.Equal(t, []seedreload.Boundary{{Key: "analytics.fx_transactions_eur", Materialized: "table", Schedule: "daily"}}, notices[0].RefreshOnSchedule)
	assert.Equal(t, []string{"analytics.fx_rates_current", "analytics.fx_rates_latest_eur"}, rebuildIDs(notices[0]))
}

// The preflight names each relation by its resolved relation when the node has one.
func TestBuildNotices_RelationsAreTheResolvedOnes(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	candSeed.ResolvedRelationID = "analytics.fx_rates_seed"
	views := fxViews()
	views[0].ResolvedRelationID = "analytics.fx_rates_now"
	_, preflights := release.BuildNotices(append(release.Topology{candSeed}, views...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	p := preflights["analytics.seed_fx_rates_eur"]
	require.NotNil(t, p)
	assert.Equal(t, "analytics.fx_rates_seed", p.Relation)
	assert.Equal(t, []string{"analytics.fx_rates_latest_eur", "analytics.fx_rates_now"}, p.Allowed)
}

func TestBuildNotices_InNodeIDOrder(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	prodB, candB := fxSeeds()
	prodB.UniqueID, candB.UniqueID = "analytics.seed_b", "analytics.seed_b"
	prodA, candA := fxSeeds()
	prodA.UniqueID, candA.UniqueID = "analytics.seed_a", "analytics.seed_a"
	notices, _ := release.BuildNotices(release.Topology{candSeed, candB, candA}, release.Topology{prodSeed, prodB, prodA}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 3)
	assert.Equal(t, []string{"analytics.seed_a", "analytics.seed_b", "analytics.seed_fx_rates_eur"},
		[]string{notices[0].NodeID, notices[1].NodeID, notices[2].NodeID})
}

func TestColumnChangesOf(t *testing.T) {
	prod := []release.SeedColumn{{Name: "a", Type: "text"}, {Name: "b", Type: "int"}, {Name: "c"}, {Name: "d"}}
	cand := []release.SeedColumn{{Name: "e"}, {Name: "c", Type: "date"}, {Name: "a", Type: "text"}, {Name: "f"}, {Name: "b", Type: "bigint"}}
	assert.Equal(t, release.ColumnChanges{Added: []string{"e", "f"}, Removed: []string{"d"}, Retyped: []string{"c", "b"}},
		release.ColumnChangesOf(prod, cand))
	assert.Equal(t, release.ColumnChanges{Added: []string{}, Removed: []string{}, Retyped: []string{}},
		release.ColumnChangesOf(nil, nil))
}

func TestNoticeApplyFindings_OnTrinoAnOutsideViewIsListedAndTheReloadGoesAhead(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	notices, preflights := release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.LateBinding, "trino", false)
	require.Len(t, notices, 1)
	n := notices[0]
	assert.Equal(t, warehouse.LateBinding, n.EngineFamily)
	assert.Equal(t, seedreload.PreflightReport, preflights["analytics.seed_fx_rates_eur"].Mode)

	n.ApplyFindings(seedreload.Findings{Checked: true, Completeness: "best_effort", Dependents: []seedreload.Dependent{
		{Schema: "BI", Name: "FX_Rates_Report", Kind: "view", Owner: "bi", Depth: 1}}})
	assert.Equal(t, "reloads", n.Prediction)
	assert.Empty(t, n.StopReason)
	assert.Equal(t, release.WarehouseFindings{Checked: true, Completeness: "best_effort",
		OutsideContinuo: []release.OutsideObject{{Object: "bi.fx_rates_report", Kind: "view", Owner: "bi"}}}, n.Warehouse)
}

func TestNoticeApplyFindings_AnUncheckedWarehouseKeepsTheReload(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	notices, _ := release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 1)
	n := notices[0]
	n.ApplyFindings(seedreload.Findings{Checked: false})
	assert.Equal(t, "reloads", n.Prediction)
	assert.Empty(t, n.StopReason)
	assert.False(t, n.Warehouse.Checked)
	assert.Equal(t, []release.OutsideObject{}, n.Warehouse.OutsideContinuo)
}

// A schema_bound warehouse that cannot list the seed's dependants cannot rule
// any out, so the load would stop.
func TestNoticeApplyFindings_NoCompletenessOnSchemaBoundStops(t *testing.T) {
	prodSeed, candSeed := fxSeeds()
	notices, _ := release.BuildNotices(append(release.Topology{candSeed}, fxViews()...), release.Topology{prodSeed}, warehouse.SchemaBound, "postgres", false)
	require.Len(t, notices, 1)
	n := notices[0]
	n.ApplyFindings(seedreload.Findings{Checked: true, Completeness: "none"})
	assert.Equal(t, "stops", n.Prediction)
	assert.Equal(t, "unowned_dependents", n.StopReason)

	// Findings that list only the views the run rebuilds put the reload back.
	n.ApplyFindings(seedreload.Findings{Checked: true, Completeness: "exact", Dependents: []seedreload.Dependent{
		{Schema: "analytics", Name: "fx_rates_current", Kind: "view", Owner: "dbt", Depth: 1, Allowed: true}}})
	assert.Equal(t, "reloads", n.Prediction)
	assert.Empty(t, n.StopReason)
}
