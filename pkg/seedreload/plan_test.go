package seedreload_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

const seedKey = "analytics.seed_fx_rates_eur"

// estate is a map-backed Graph built from nodes and their upstream keys.
type estate struct {
	nodes map[string]seedreload.Node
	up    map[string][]string
}

func (e estate) Node(key string) (seedreload.Node, bool) {
	n, ok := e.nodes[key]
	return n, ok
}

func (e estate) Upstreams(key string) []string {
	out := slices.Clone(e.up[key])
	sort.Strings(out)
	return out
}

func (e estate) Downstreams(key string) []string {
	var out []string
	for k, ups := range e.up {
		if slices.Contains(ups, key) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func fxEstate() estate {
	e := estate{nodes: map[string]seedreload.Node{}, up: map[string][]string{}}
	add := func(key, nodeType, materialized, schedule string, upstreams ...string) {
		e.nodes[key] = seedreload.Node{Key: key, NodeType: nodeType, Materialized: materialized, Schedule: schedule, Relation: key}
		e.up[key] = upstreams
	}
	add(seedKey, "dbt-seed", "seed", "seed")
	add("analytics.fx_rates_current", "dbt-model", "view", "hourly", seedKey)
	add("analytics.fx_rates_latest_eur", "dbt-model", "view", "hourly", "analytics.fx_rates_current")
	add("analytics.fx_rates_mv", "dbt-model", "materialized_view", "", "analytics.fx_rates_current")
	add("analytics.fx_rates_clean", "dbt-model", "ephemeral", "", seedKey)
	add("analytics.fx_rates_clean_view", "dbt-model", "view", "", seedKey, "analytics.fx_rates_clean")
	add("analytics.fx_transactions_eur", "dbt-model", "table", "daily", seedKey)
	add("analytics.fx_daily", "dbt-model", "incremental", "", "analytics.fx_rates_current")
	add("analytics.fx_snap", "dbt-snapshot", "snapshot", "", "analytics.fx_rates_current")
	add("analytics.py_fx", "python-node", "", "", "analytics.fx_rates_current")
	n := e.nodes["analytics.fx_rates_current"]
	n.Relation = "analytics.fx_rates_now"
	e.nodes["analytics.fx_rates_current"] = n
	return e
}

func keysOf(rs []seedreload.Rebuild) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Key)
	}
	return out
}

func rebuildsByKey(p seedreload.Plan) map[string]seedreload.Rebuild {
	out := map[string]seedreload.Rebuild{}
	for _, r := range p.Rebuilds {
		out[r.Key] = r
	}
	return out
}

func TestPlanSeed_TheLoad(t *testing.T) {
	yes, no := true, false
	pg, tr := warehouse.SchemaBound, warehouse.LateBinding
	full, run := model.OperationFullRefresh, model.OperationRun
	disabled := model.FailureReasonSeedFullRefreshDisabled
	for _, c := range []struct {
		name    string
		family  warehouse.Family
		setting *bool
		change  seedreload.Change
		trigger seedreload.Trigger
		want    seedreload.Load
	}{
		{"pg unset rows", pg, nil, seedreload.ChangeRows, seedreload.TriggerLoad, seedreload.Load{Operation: run}},
		{"pg unset columns", pg, nil, seedreload.ChangeColumns, seedreload.TriggerLoad, seedreload.Load{Operation: full, DropsTable: true, Reason: "columns changed"}},
		{"pg true rows", pg, &yes, seedreload.ChangeRows, seedreload.TriggerLoad, seedreload.Load{Operation: run, DropsTable: true, Reason: "+full_refresh: true re-creates the table on every load"}},
		{"pg true columns", pg, &yes, seedreload.ChangeColumns, seedreload.TriggerLoad, seedreload.Load{Operation: run, DropsTable: true, Reason: "columns changed; +full_refresh: true re-creates the table on every load"}},
		{"pg false rows", pg, &no, seedreload.ChangeRows, seedreload.TriggerLoad, seedreload.Load{Operation: run}},
		{"pg false columns", pg, &no, seedreload.ChangeColumns, seedreload.TriggerLoad, seedreload.Load{Operation: full, Refusal: disabled, Reason: "columns changed; +full_refresh: false keeps the table, so no command can apply the change"}},
		{"pg unset operator", pg, nil, seedreload.ChangeRows, seedreload.TriggerFullRefresh, seedreload.Load{Operation: full, DropsTable: true, Reason: "full refresh requested by an operator"}},
		{"pg true operator", pg, &yes, seedreload.ChangeRows, seedreload.TriggerFullRefresh, seedreload.Load{Operation: run, DropsTable: true, Reason: "full refresh requested by an operator; +full_refresh: true re-creates the table on every load"}},
		{"pg false operator", pg, &no, seedreload.ChangeRows, seedreload.TriggerFullRefresh, seedreload.Load{Operation: full, Refusal: disabled, Reason: "+full_refresh: false keeps the table, so a full refresh cannot rebuild it"}},
		{"trino rows", tr, nil, seedreload.ChangeRows, seedreload.TriggerLoad, seedreload.Load{Operation: run, DropsTable: true, Reason: "on Trino every load re-creates the table"}},
		{"trino false columns", tr, &no, seedreload.ChangeColumns, seedreload.TriggerLoad, seedreload.Load{Operation: run, DropsTable: true, Reason: "columns changed; on Trino every load re-creates the table"}},
		{"trino operator", tr, &yes, seedreload.ChangeRows, seedreload.TriggerFullRefresh, seedreload.Load{Operation: run, DropsTable: true, Reason: "full refresh requested by an operator; on Trino every load re-creates the table"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			seed := fxEstate()
			s := seed.nodes[seedKey]
			s.FullRefresh = c.setting
			seed.nodes[seedKey] = s
			engine := map[warehouse.Family]string{pg: "postgres", tr: "trino"}[c.family]
			got := seedreload.PlanSeed(seedreload.Request{Seed: s, Family: c.family, Engine: engine, Change: c.change, Trigger: c.trigger}, seed)
			assert.Equal(t, c.want, got.Load)
		})
	}
}

func TestPlanSeed_RebuildsTheViewsBelowTheSeedInOrder(t *testing.T) {
	g := fxEstate()
	yes := true
	seed := g.nodes[seedKey]
	seed.FullRefresh = &yes
	p := seedreload.PlanSeed(seedreload.Request{Seed: seed, Family: warehouse.SchemaBound, Engine: "postgres", Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad}, g)
	assert.Equal(t, []string{"analytics.fx_rates_clean_view", "analytics.fx_rates_current", "analytics.fx_rates_latest_eur", "analytics.fx_rates_mv"}, keysOf(p.Rebuilds))
	byKey := rebuildsByKey(p)
	assert.Equal(t, seedreload.Rebuild{Key: "analytics.fx_rates_current", Materialized: "view", Schedule: "hourly", Relation: "analytics.fx_rates_now",
		Operation: model.OperationRun, Reason: "rebuild: dropped by the load of analytics.seed_fx_rates_eur"}, byKey["analytics.fx_rates_current"])
	assert.Equal(t, "rebuild: dropped by the load of analytics.seed_fx_rates_eur, through analytics.fx_rates_current", byKey["analytics.fx_rates_latest_eur"].Reason)
	assert.Equal(t, model.OperationRun, byKey["analytics.fx_rates_mv"].Operation)
	assert.Equal(t, []seedreload.Boundary{
		{Key: "analytics.fx_daily", Materialized: "incremental"},
		{Key: "analytics.fx_snap", Materialized: "snapshot"},
		{Key: "analytics.fx_transactions_eur", Materialized: "table", Schedule: "daily"},
		{Key: "analytics.py_fx", Materialized: "python-node"},
	}, p.RefreshOnSchedule)
}

func TestPlanSeed_AnEphemeralNodeIsWalkedThroughWithoutARebuild(t *testing.T) {
	g := fxEstate()
	g.nodes["analytics.fx_via_clean"] = seedreload.Node{Key: "analytics.fx_via_clean", NodeType: "dbt-model", Materialized: "view", Relation: "analytics.fx_via_clean"}
	g.up["analytics.fx_via_clean"] = []string{"analytics.fx_rates_clean"}
	g.nodes["analytics.fx_clean_table"] = seedreload.Node{Key: "analytics.fx_clean_table", NodeType: "dbt-model", Materialized: "table", Relation: "analytics.fx_clean_table"}
	g.up["analytics.fx_clean_table"] = []string{"analytics.fx_rates_clean"}
	yes := true
	seed := g.nodes[seedKey]
	seed.FullRefresh = &yes
	p := seedreload.PlanSeed(seedreload.Request{Seed: seed, Family: warehouse.SchemaBound, Engine: "postgres", Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad}, g)
	byKey := rebuildsByKey(p)
	require.Contains(t, byKey, "analytics.fx_via_clean")
	assert.Equal(t, "rebuild: dropped by the load of analytics.seed_fx_rates_eur", byKey["analytics.fx_via_clean"].Reason)
	assert.NotContains(t, byKey, "analytics.fx_rates_clean")
	assert.Contains(t, p.RefreshOnSchedule, seedreload.Boundary{Key: "analytics.fx_clean_table", Materialized: "table"})
}

func TestPlanSeed_OnALateBindingEngine(t *testing.T) {
	g := fxEstate()
	p := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.LateBinding, Engine: "trino", Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad}, g)
	byKey := rebuildsByKey(p)
	assert.Equal(t, model.OperationFullRefresh, byKey["analytics.fx_rates_mv"].Operation)
	assert.Equal(t, model.OperationRun, byKey["analytics.fx_rates_current"].Operation)
	assert.Equal(t, "rebuild: may be stale on Trino after the load of analytics.seed_fx_rates_eur", byKey["analytics.fx_rates_current"].Reason)
	assert.Equal(t, "rebuild: may be stale on Trino after the load of analytics.seed_fx_rates_eur, through analytics.fx_rates_current", byKey["analytics.fx_rates_mv"].Reason)
}

func TestPlanSeed_ALoadThatDropsNothingOrIsRefusedRebuildsNothing(t *testing.T) {
	g := fxEstate()
	no := false
	rows := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.SchemaBound, Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad}, g)
	assert.Empty(t, rows.Rebuilds)
	assert.Empty(t, rows.RefreshOnSchedule)
	seed := g.nodes[seedKey]
	seed.FullRefresh = &no
	refused := seedreload.PlanSeed(seedreload.Request{Seed: seed, Family: warehouse.SchemaBound, Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad}, g)
	assert.Empty(t, refused.Rebuilds)
	assert.Nil(t, refused.Preflight(nil))
}

func TestPlanSeed_AColumnChangeOfABootstrapReleaseIsUnvalidated(t *testing.T) {
	g := fxEstate()
	cols := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.SchemaBound, Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad, Unvalidated: true}, g)
	assert.Equal(t, "columns changed; unvalidated: its release skipped validation", cols.Load.Reason)
	rows := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.LateBinding, Engine: "trino", Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad, Unvalidated: true}, g)
	assert.Equal(t, "on Trino every load re-creates the table", rows.Load.Reason)
}

func TestPlanSeed_TheUnvalidatedNoticeEndsTheReason(t *testing.T) {
	g := fxEstate()
	yes, no := true, false
	seed := g.nodes[seedKey]
	seed.FullRefresh = &yes
	withTrue := seedreload.PlanSeed(seedreload.Request{Seed: seed, Family: warehouse.SchemaBound, Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad, Unvalidated: true}, g)
	assert.Equal(t, "columns changed; +full_refresh: true re-creates the table on every load; unvalidated: its release skipped validation", withTrue.Load.Reason)
	trino := seedreload.PlanSeed(seedreload.Request{Seed: seed, Family: warehouse.LateBinding, Engine: "trino", Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad, Unvalidated: true}, g)
	assert.Equal(t, "columns changed; on Trino every load re-creates the table; unvalidated: its release skipped validation", trino.Load.Reason)
	seed.FullRefresh = &no
	refused := seedreload.PlanSeed(seedreload.Request{Seed: seed, Family: warehouse.SchemaBound, Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad, Unvalidated: true}, g)
	assert.Equal(t, "columns changed; +full_refresh: false keeps the table, so no command can apply the change", refused.Load.Reason)
}

func TestPlanSeed_RebuildsAreOrderedAfterTheViewsTheyRead(t *testing.T) {
	g := estate{nodes: map[string]seedreload.Node{}, up: map[string][]string{}}
	add := func(key, materialized string, upstreams ...string) {
		g.nodes[key] = seedreload.Node{Key: key, NodeType: "dbt-model", Materialized: materialized, Relation: key}
		g.up[key] = upstreams
	}
	add(seedKey, "seed")
	add("s.zeta", "view", seedKey)
	add("s.alpha", "view", "s.zeta")
	add("s.beta", "view", "s.alpha", "s.zeta")
	p := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.LateBinding, Engine: "trino", Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad}, g)
	assert.Equal(t, []string{"s.zeta", "s.alpha", "s.beta"}, keysOf(p.Rebuilds))
	assert.Equal(t, "rebuild: may be stale on Trino after the load of analytics.seed_fx_rates_eur, through s.alpha", p.Rebuilds[2].Reason)
	assert.Equal(t, []seedreload.Boundary{}, p.RefreshOnSchedule)
}

func TestPreflight(t *testing.T) {
	g := fxEstate()
	pg := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.SchemaBound, Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad}, g)
	assert.Equal(t, &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur",
		Allowed:      []string{"analytics.fx_rates_clean_view", "analytics.fx_rates_latest_eur", "analytics.fx_rates_mv", "analytics.fx_rates_now"},
		Acknowledged: []string{"bi.fx_rates_report"}}, pg.Preflight([]string{" BI.Fx_Rates_Report ", "bi.fx_rates_report", ""}))
	assert.Equal(t, seedreload.PreflightReport, pg.ReportPreflight().Mode)
	assert.Empty(t, pg.ReportPreflight().Acknowledged)
	tr := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.LateBinding, Engine: "trino", Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad}, g)
	assert.Equal(t, seedreload.PreflightReport, tr.Preflight(nil).Mode)
	rows := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.SchemaBound, Change: seedreload.ChangeRows, Trigger: seedreload.TriggerLoad}, g)
	assert.Nil(t, rows.Preflight(nil))
	assert.Nil(t, rows.ReportPreflight())
}

func TestPreflight_ListsAreNeverNil(t *testing.T) {
	g := estate{nodes: map[string]seedreload.Node{}, up: map[string][]string{}}
	g.nodes[seedKey] = seedreload.Node{Key: seedKey, NodeType: "dbt-seed", Materialized: "seed", Relation: seedKey}
	p := seedreload.PlanSeed(seedreload.Request{Seed: g.nodes[seedKey], Family: warehouse.SchemaBound, Change: seedreload.ChangeColumns, Trigger: seedreload.TriggerLoad}, g)
	assert.NotNil(t, p.Rebuilds)
	assert.NotNil(t, p.RefreshOnSchedule)
	pf := p.Preflight(nil)
	require.NotNil(t, pf)
	assert.Equal(t, []string{}, pf.Allowed)
	assert.Equal(t, []string{}, pf.Acknowledged)
}
