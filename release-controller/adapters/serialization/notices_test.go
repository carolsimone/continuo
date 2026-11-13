package serialization_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func designNotice() release.Notice {
	return release.Notice{
		Kind: "seed_reload", NodeID: "analytics.seed_fx_rates_eur", Service: "finance",
		EngineFamily: warehouse.SchemaBound, Change: seedreload.ChangeColumns,
		Columns: release.ColumnChanges{Added: []string{"source"}, Removed: []string{}, Retyped: []string{}},
		Load:    seedreload.Load{Operation: model.OperationRun, DropsTable: true, Reason: "columns changed; +full_refresh: true re-creates the table on every load"},
		Rebuilds: []seedreload.Rebuild{
			{Key: "analytics.fx_rates_current", Materialized: "view", Schedule: "hourly"},
			{Key: "analytics.fx_rates_latest_eur", Materialized: "view", Schedule: "hourly"},
		},
		RefreshOnSchedule: []seedreload.Boundary{{Key: "analytics.fx_transactions_eur", Materialized: "table", Schedule: "daily"}},
		Warehouse: release.WarehouseFindings{Checked: true, Completeness: "exact",
			OutsideContinuo: []release.OutsideObject{{Object: "bi.fx_rates_report", Kind: "view", Owner: "bi"}}},
		Prediction: "stops", StopReason: "unowned_dependents",
	}
}

// The notice's JSON is the design's shape, on GET /releases/{id} and in the
// release_pipeline_runs.notices column.
func TestNoticesFromDomain_IsTheDesignsJSON(t *testing.T) {
	raw, err := json.Marshal(serialization.NoticesFromDomain([]release.Notice{designNotice()}))
	require.NoError(t, err)
	assert.JSONEq(t, `[{"kind": "seed_reload", "node_id": "analytics.seed_fx_rates_eur", "service": "finance",
  "engine_family": "schema_bound", "change": "columns",
  "columns": {"added": ["source"], "removed": [], "retyped": []},
  "load": {"operation": "run", "drops_table": true, "reason": "columns changed; +full_refresh: true re-creates the table on every load"},
  "rebuilds": [{"node_id": "analytics.fx_rates_current", "materialized": "view", "operation": "run", "schedule": "hourly"},
               {"node_id": "analytics.fx_rates_latest_eur", "materialized": "view", "operation": "run", "schedule": "hourly"}],
  "refresh_on_schedule": [{"node_id": "analytics.fx_transactions_eur", "materialized": "table", "schedule": "daily"}],
  "warehouse": {"checked": true, "completeness": "exact",
                "outside_continuo": [{"object": "bi.fx_rates_report", "kind": "view", "owner": "bi"}]},
  "prediction": "stops", "stop_reason": "unowned_dependents", "unvalidated": false}]`, string(raw))
}

func TestNoticesFromDomain_EmptyListsAreArrays(t *testing.T) {
	raw, err := json.Marshal(serialization.NoticesFromDomain(nil))
	require.NoError(t, err)
	assert.JSONEq(t, `[]`, string(raw))

	raw, err = json.Marshal(serialization.NoticesFromDomain([]release.Notice{{Kind: "seed_reload", NodeID: "s"}}))
	require.NoError(t, err)
	var got []map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, []any{}, got[0]["rebuilds"])
	assert.Equal(t, []any{}, got[0]["refresh_on_schedule"])
	assert.Equal(t, map[string]any{"added": []any{}, "removed": []any{}, "retyped": []any{}}, got[0]["columns"])
	assert.Equal(t, []any{}, got[0]["warehouse"].(map[string]any)["outside_continuo"])
}

func TestNoticesToDomain_RoundTrips(t *testing.T) {
	stopped := designNotice()
	fullRefresh := designNotice()
	fullRefresh.NodeID = "analytics.seed_b"
	fullRefresh.Load.Operation = model.OperationFullRefresh
	fullRefresh.Rebuilds[0].Operation = model.OperationFullRefresh
	refused := designNotice()
	refused.NodeID = "analytics.seed_c"
	refused.Load = seedreload.Load{Operation: model.OperationFullRefresh, Refusal: model.FailureReasonSeedFullRefreshDisabled,
		Reason: "columns changed; +full_refresh: false keeps the table, so no command can apply the change"}
	refused.Rebuilds, refused.RefreshOnSchedule = []seedreload.Rebuild{}, []seedreload.Boundary{}
	refused.Prediction, refused.StopReason = "refused", "seed_full_refresh_disabled"
	in := []release.Notice{stopped, fullRefresh, refused}

	raw, err := json.Marshal(serialization.NoticesFromDomain(in))
	require.NoError(t, err)
	var dtos []serialization.NoticeDTO
	require.NoError(t, json.Unmarshal(raw, &dtos))
	assert.Equal(t, in, serialization.NoticesToDomain(dtos))
	assert.Equal(t, []release.Notice{}, serialization.NoticesToDomain(nil))
}
