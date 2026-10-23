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

func loadTaskDispatchFixture(t *testing.T) taskDispatchFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/task_dispatch_v1.json")
	require.NoError(t, err)
	var fx taskDispatchFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func TestTaskDispatchFields_MatchGoldenFixture(t *testing.T) {
	fx := loadTaskDispatchFixture(t)
	got, err := events.TaskDispatchFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestDecodeTaskDispatch_ReadsGoldenFixture(t *testing.T) {
	fx := loadTaskDispatchFixture(t)
	env, p, err := events.DecodeTaskDispatch(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload, p)
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
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
		"producer":       "state",
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
