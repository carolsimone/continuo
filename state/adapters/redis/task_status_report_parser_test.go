package redis

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reportEntry reads the golden task.status.updated:v2 entry of status as the
// Redis message state receives: every field a string, the payload JSON.
func reportEntry(t *testing.T, status string) goredis.XMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", "task_status_updated_v2_"+status+".json")) //nolint:gosec // G304: a fixed fixture path
	require.NoError(t, err)
	var fx struct {
		Fields map[string]json.RawMessage `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(raw, &fx))
	values := make(map[string]any, len(fx.Fields))
	for k, v := range fx.Fields {
		if k == "payload" {
			values[k] = string(v)
			continue
		}
		var s string
		require.NoError(t, json.Unmarshal(v, &s))
		values[k] = s
	}
	return goredis.XMessage{ID: "1-0", Values: values}
}

func TestParseTaskStatusReport_ReadsTheGoldenEntries(t *testing.T) {
	for _, status := range []string{"queued", "running", "succeeded", "failed"} {
		t.Run(status, func(t *testing.T) {
			got, err := ParseTaskStatusReport(reportEntry(t, status))
			require.NoError(t, err)
			assert.Equal(t, pkgevents.DefaultTenantID, got.TenantID, "the report carries its envelope's tenant")
			assert.Equal(t, uuid.MustParse("7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b"), got.RunID)
			assert.Equal(t, uuid.MustParse("3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"), got.Report.TaskID)
			assert.Equal(t, run.AttemptStatus(status), got.Report.Status)
			assert.Equal(t, "service-1-e2e-schema-ftable-e-7f3e1a2b", got.Report.JobName)
			require.NotNil(t, got.Report.QueuedAt, "every report carries the facts known so far")
		})
	}
}

func TestParseTaskStatusReport_AFailureCarriesItsReasonAndFacts(t *testing.T) {
	got, err := ParseTaskStatusReport(reportEntry(t, "failed"))
	require.NoError(t, err)
	r := got.Report
	assert.Equal(t, int32(0), r.Attempt)
	assert.Equal(t, model.FailureReasonOOMKilled, r.FailureReason)
	assert.Equal(t, model.FailureClassResource, r.FailureReason.Class())
	require.NotNil(t, r.ExitCode)
	assert.Equal(t, int32(137), *r.ExitCode)
	require.NotNil(t, r.ErrorMessage)
	assert.Equal(t, "OOMKilled: the container exceeded its memory", *r.ErrorMessage)
	assert.Nil(t, r.RunResultsURI, "a fact the report leaves empty is not reported")
	assert.Equal(t, time.Date(2026, 10, 20, 23, 1, 24, 105000000, time.UTC), r.StartedAt.UTC())
}

func TestParseTaskStatusReport_AnEntryItCannotTrustIsPermanent(t *testing.T) {
	cases := map[string]func(map[string]any){
		"no attempt": func(v map[string]any) {
			v["payload"] = `{"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a","status":"queued"}`
		},
		"a failure without a reason": func(v map[string]any) {
			v["payload"] = `{"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a","attempt":0,"status":"failed"}`
		},
		"another schema version": func(v map[string]any) { v["schema_version"] = "2" },
		"no payload":             func(v map[string]any) { delete(v, "payload") },
		"no tenant":              func(v map[string]any) { delete(v, "tenant_id") },
		"an empty tenant":        func(v map[string]any) { v["tenant_id"] = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			msg := reportEntry(t, "queued")
			mutate(msg.Values)
			_, err := ParseTaskStatusReport(msg)
			require.ErrorIs(t, err, pkgevents.ErrPermanent)
		})
	}
}
