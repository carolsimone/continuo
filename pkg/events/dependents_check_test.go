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

func TestDependentsCheck_RoundTripsTheDomain(t *testing.T) {
	d := &seedreload.DependentsCheck{Mode: seedreload.CheckMode, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur",
		Allowed: []string{"analytics.fx_rates_current"}, Acknowledged: []string{}}
	assert.Equal(t, d, events.DependentsCheckOf(d).ToDomain())
	assert.Nil(t, events.DependentsCheckOf(nil))
	assert.Nil(t, (*events.DependentsCheck)(nil).ToDomain())
}

func TestDependentsCheck_RendersEmptyListsAsArrays(t *testing.T) {
	raw, err := json.Marshal(events.DependentsCheckOf(&seedreload.DependentsCheck{Mode: seedreload.ReportMode, Family: warehouse.LateBinding, Relation: "a.s"}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"mode":"report","engine_family":"late_binding","relation":"a.s","allowed":[],"acknowledged":[]}`, string(raw))
}

func TestDependentsCheckFindings_RoundTripsTheDomain(t *testing.T) {
	d := &seedreload.Findings{
		DropSemantics: "cascade", Completeness: "exact", Checked: true,
		Dependents: []seedreload.Dependent{
			{Schema: "analytics", Name: "fx_rates_current", Kind: "view", Owner: "dbt", Depth: 1, Allowed: true},
			{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1},
		},
		Probes: []seedreload.Probe{{Schema: "bi", Name: "fx_rates_report", Error: "VIEW_IS_STALE"}},
	}
	assert.Equal(t, d, events.DependentsCheckFindingsOf(d).ToDomain())
	assert.Nil(t, events.DependentsCheckFindingsOf(nil))
	assert.Nil(t, (*events.DependentsCheckFindings)(nil).ToDomain())
}

func TestDependentsCheckFindings_RendersNoDependentsAsAnArrayAndOmitsNoProbes(t *testing.T) {
	raw, err := json.Marshal(events.DependentsCheckFindingsOf(&seedreload.Findings{DropSemantics: "late_binding", Completeness: "best_effort", Checked: true}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"drop_semantics":"late_binding","completeness":"best_effort","checked":true,"dependents":[]}`, string(raw))
}

// A payload that names no list, or names it null, marshals it as `[]` again:
// the outbox publisher and the last-report resend decode a payload and encode
// it back.
func TestDependentsCheck_ADecodedPayloadWithoutListsMarshalsEmptyArrays(t *testing.T) {
	for _, in := range []string{
		`{"mode":"report","engine_family":"late_binding","relation":"a.s"}`,
		`{"mode":"report","engine_family":"late_binding","relation":"a.s","allowed":null,"acknowledged":null}`,
	} {
		var p events.DependentsCheck
		require.NoError(t, json.Unmarshal([]byte(in), &p))
		raw, err := json.Marshal(p)
		require.NoError(t, err)
		assert.JSONEq(t, `{"mode":"report","engine_family":"late_binding","relation":"a.s","allowed":[],"acknowledged":[]}`, string(raw))

		raw, err = json.Marshal(events.TaskDispatch{DependentsCheck: &p})
		require.NoError(t, err)
		assert.Contains(t, string(raw), `"allowed":[],"acknowledged":[]`)
	}
}

func TestDependentsCheckFindings_ADecodedPayloadWithoutDependentsMarshalsAnEmptyArray(t *testing.T) {
	for _, in := range []string{
		`{"drop_semantics":"late_binding","completeness":"best_effort","checked":true}`,
		`{"drop_semantics":"late_binding","completeness":"best_effort","checked":true,"dependents":null}`,
	} {
		var f events.DependentsCheckFindings
		require.NoError(t, json.Unmarshal([]byte(in), &f))
		raw, err := json.Marshal(f)
		require.NoError(t, err)
		assert.JSONEq(t, `{"drop_semantics":"late_binding","completeness":"best_effort","checked":true,"dependents":[]}`, string(raw))

		raw, err = json.Marshal(events.TaskStatusReport{DependentsCheck: &f})
		require.NoError(t, err)
		assert.Contains(t, string(raw), `"dependents":[]`)
	}
}
