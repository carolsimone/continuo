package events_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
)

type taskDispatchFixture struct {
	Input struct {
		EventID    string              `json:"event_id"`
		TenantID   string              `json:"tenant_id"`
		Producer   string              `json:"producer"`
		OccurredAt time.Time           `json:"occurred_at"`
		Payload    events.TaskDispatch `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

var taskDispatchFixtures = []string{
	"testdata/task_dispatch_v1.json",
	"testdata/task_dispatch_v1_seed_preflight.json",
}

func loadTaskDispatchFixture(t *testing.T, path string) taskDispatchFixture {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is one of the fixed fixture paths above
	require.NoError(t, err)
	var fx taskDispatchFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func TestTaskDispatchFields_MatchGoldenFixtures(t *testing.T) {
	for _, path := range taskDispatchFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadTaskDispatchFixture(t, path)
			got, err := events.TaskDispatchFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
			require.NoError(t, err)
			assertFieldsMatchFixture(t, fx.Fields, got)
		})
	}
}

func TestDecodeTaskDispatch_ReadsGoldenFixtures(t *testing.T) {
	for _, path := range taskDispatchFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadTaskDispatchFixture(t, path)
			env, p, err := events.DecodeTaskDispatch(fixtureFields(t, fx.Fields))
			require.NoError(t, err)
			assert.Equal(t, fx.Input.Payload, p)
			assert.Equal(t, fx.Input.EventID, env.EventID)
			assert.Equal(t, fx.Input.TenantID, env.TenantID)
			assert.Equal(t, fx.Input.Producer, env.Producer)
		})
	}
}

func TestTaskDispatch_DecodesThePlanReasonAndTheSeedPreflight(t *testing.T) {
	decode := func(path string) events.TaskDispatch {
		_, p, err := events.DecodeTaskDispatch(fixtureFields(t, loadTaskDispatchFixture(t, path).Fields))
		require.NoError(t, err)
		return p
	}
	p := decode("testdata/task_dispatch_v1_seed_preflight.json")
	assert.Equal(t, "columns changed; +full_refresh: true re-creates the table on every load", p.PlanReason)
	assert.Equal(t, &events.SeedPreflight{Mode: "check", EngineFamily: "schema_bound", Relation: "analytics.seed_fx_rates_eur",
		Allowed: []string{"analytics.fx_rates_current", "analytics.fx_rates_latest_eur"}, Acknowledged: []string{}}, p.SeedPreflight)

	plain := decode("testdata/task_dispatch_v1.json")
	assert.Nil(t, plain.SeedPreflight)
	assert.Empty(t, plain.PlanReason)
}

func TestDecodeTaskDispatch_ReadsAttemptZero(t *testing.T) {
	fields := validDispatchFields(`"attempt":0`)
	_, p, err := events.DecodeTaskDispatch(fields)
	require.NoError(t, err)
	assert.Equal(t, int32(0), p.Attempt)
}

// The identity of the attempt (run, task, attempt number) decides where a
// dispatch lands; an entry without a readable one can never be applied.
func TestDecodeTaskDispatch_RejectsAnEntryWithoutAnAttemptIdentity(t *testing.T) {
	cases := map[string]map[string]string{
		"no attempt":          validDispatchFields(``),
		"attempt not integer": validDispatchFields(`"attempt":1.5`),
		"attempt as string":   validDispatchFields(`"attempt":"1"`),
		"negative attempt":    validDispatchFields(`"attempt":-1`),
		"task_id not a uuid":  withPayload(validDispatchFields(`"attempt":0`), `{"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"t1","attempt":0}`),
		"run_id not a uuid":   withPayload(validDispatchFields(`"attempt":0`), `{"run_id":"r1","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a","attempt":0}`),
		"payload not json":    withPayload(validDispatchFields(`"attempt":0`), `{`),
		"other version":       withField(validDispatchFields(`"attempt":0`), "schema_version", "2"),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeTaskDispatch(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

// A dispatch whose identity reads keeps its other fields as written: the
// consumer reports the attempt failed when one of them cannot run.
func TestDecodeTaskDispatch_KeepsUnjudgedFieldsAsWritten(t *testing.T) {
	_, p, err := events.DecodeTaskDispatch(validDispatchFields(`"attempt":2,"known_status":"lost","node_type":"dbt-macro"`))
	require.NoError(t, err)
	assert.Equal(t, model.AttemptStatus("lost"), p.KnownStatus)
	assert.Equal(t, "dbt-macro", p.NodeType)
}

func validDispatchFields(extra string) map[string]string {
	payload := `{"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"`
	if extra != "" {
		payload += "," + extra
	}
	return map[string]string{
		"event_id":       "e",
		"tenant_id":      "default",
		"occurred_at":    "2026-10-20T23:01:52.000000Z",
		"producer":       "run-controller",
		"schema_version": "1",
		"payload":        payload + "}",
	}
}

func withPayload(fields map[string]string, payload string) map[string]string {
	return withField(fields, "payload", payload)
}

func withField(fields map[string]string, key, value string) map[string]string {
	fields[key] = value
	return fields
}

func TestDecodeTaskDispatch_ReadsContentHash(t *testing.T) {
	_, p, err := events.DecodeTaskDispatch(validDispatchFields(`"attempt":0,"content_hash":"sha256:91d4c0"`))
	require.NoError(t, err)
	assert.Equal(t, "sha256:91d4c0", p.ContentHash)
}

func TestTaskDispatch_OmitsAnEmptyContentHash(t *testing.T) {
	raw, err := json.Marshal(events.TaskDispatch{RunID: "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b", TaskID: "3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"})
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "content_hash")
}
