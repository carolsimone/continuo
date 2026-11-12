package events_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

func TestSeedPreflight_RoundTripsTheDomain(t *testing.T) {
	d := &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur",
		Allowed: []string{"analytics.fx_rates_current"}, Acknowledged: []string{}}
	assert.Equal(t, d, events.SeedPreflightOf(d).ToDomain())
	assert.Nil(t, events.SeedPreflightOf(nil))
	assert.Nil(t, (*events.SeedPreflight)(nil).ToDomain())
}

func TestSeedPreflight_RendersEmptyListsAsArrays(t *testing.T) {
	raw, err := json.Marshal(events.SeedPreflightOf(&seedreload.Preflight{Mode: seedreload.PreflightReport, Family: warehouse.LateBinding, Relation: "a.s"}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"mode":"report","engine_family":"late_binding","relation":"a.s","allowed":[],"acknowledged":[]}`, string(raw))
}

func TestSeedPreflightFindings_RoundTripsTheDomain(t *testing.T) {
	d := &seedreload.Findings{
		DropSemantics: "cascade", Completeness: "exact", Checked: true,
		Dependents: []seedreload.Dependent{
			{Schema: "analytics", Name: "fx_rates_current", Kind: "view", Owner: "dbt", Depth: 1, Allowed: true},
			{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1},
		},
		Probes: []seedreload.Probe{{Schema: "bi", Name: "fx_rates_report", Error: "VIEW_IS_STALE"}},
	}
	assert.Equal(t, d, events.SeedPreflightFindingsOf(d).ToDomain())
	assert.Nil(t, events.SeedPreflightFindingsOf(nil))
	assert.Nil(t, (*events.SeedPreflightFindings)(nil).ToDomain())
}

func TestSeedPreflightFindings_RendersNoDependentsAsAnArrayAndOmitsNoProbes(t *testing.T) {
	raw, err := json.Marshal(events.SeedPreflightFindingsOf(&seedreload.Findings{DropSemantics: "late_binding", Completeness: "best_effort", Checked: true}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"drop_semantics":"late_binding","completeness":"best_effort","checked":true,"dependents":[]}`, string(raw))
}
