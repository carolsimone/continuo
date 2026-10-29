package events_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/events"
)

var runCancelledFixtures = []string{
	"testdata/run_cancelled_v1.json",
	"testdata/run_cancelled_v1_attempts.json",
}

type runCancelledFixture struct {
	Input struct {
		EventID    string              `json:"event_id"`
		TenantID   string              `json:"tenant_id"`
		Producer   string              `json:"producer"`
		OccurredAt time.Time           `json:"occurred_at"`
		Payload    events.RunCancelled `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func loadRunCancelledFixture(t *testing.T, path string) runCancelledFixture {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is one of the fixed fixture paths above
	require.NoError(t, err)
	var fx runCancelledFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func TestRunCancelledFields_MatchGoldenFixtures(t *testing.T) {
	for _, path := range runCancelledFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadRunCancelledFixture(t, path)
			got, err := events.RunCancelledFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
			require.NoError(t, err)
			assertFieldsMatchFixture(t, fx.Fields, got)
		})
	}
}

// A cancel with no attempt in flight still carries attempts as an array.
func TestRunCancelledFields_WritesNoAttemptsAsAnEmptyArray(t *testing.T) {
	got, err := events.RunCancelledFields("e", "default", "run-controller", time.Date(2026, 10, 23, 9, 14, 7, 0, time.UTC), events.RunCancelled{
		RunID: "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b", CancelledBy: "alice@example.com",
	})
	require.NoError(t, err)
	assert.Contains(t, got["payload"], `"attempts":[]`)
}

func TestDecodeRunCancelled_ReadsGoldenFixtures(t *testing.T) {
	for _, path := range runCancelledFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadRunCancelledFixture(t, path)
			env, p, err := events.DecodeRunCancelled(fixtureFields(t, fx.Fields))
			require.NoError(t, err)
			assert.Equal(t, fx.Input.Payload, p)
			assert.Equal(t, fx.Input.EventID, env.EventID)
			assert.Equal(t, fx.Input.TenantID, env.TenantID)
			assert.Equal(t, fx.Input.Producer, env.Producer)
			assert.Equal(t, events.RunCancelledSchemaVersion, env.SchemaVersion)
		})
	}
}

// A cancel that does not name its run, or an attempt it must stop, can never
// be applied.
func TestDecodeRunCancelled_RejectsUnreadableCancels(t *testing.T) {
	const run = `"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b"`
	const task = `"task_id":"3e41c7d2-5a98-4b36-8f10-2d7e9a4c6b15"`
	cases := map[string]map[string]string{
		"no envelope":          {"payload": `{` + run + `,"attempts":[]}`},
		"other schema version": withField(cancelledFields(run+`,"attempts":[]`), "schema_version", "2"),
		"payload not json":     withPayload(cancelledFields(``), `{`),
		"no run_id":            cancelledFields(`"attempts":[]`),
		"run_id not a uuid":    cancelledFields(`"run_id":"r1","attempts":[]`),
		"task_id not a uuid":   cancelledFields(run + `,"attempts":[{"task_id":"t1","attempt":0,"job_name":"j"}]`),
		"no task_id":           cancelledFields(run + `,"attempts":[{"attempt":0,"job_name":"j"}]`),
		"no attempt":           cancelledFields(run + `,"attempts":[{` + task + `,"job_name":"j"}]`),
		"attempt not integer":  cancelledFields(run + `,"attempts":[{` + task + `,"attempt":"0","job_name":"j"}]`),
		"negative attempt":     cancelledFields(run + `,"attempts":[{` + task + `,"attempt":-1,"job_name":"j"}]`),
		"empty job name":       cancelledFields(run + `,"attempts":[{` + task + `,"attempt":0,"job_name":""}]`),
		"no job name":          cancelledFields(run + `,"attempts":[{` + task + `,"attempt":0}]`),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeRunCancelled(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

// A payload that omits attempts reads as a cancel with none in flight.
func TestDecodeRunCancelled_ReadsMissingAttemptsAsNone(t *testing.T) {
	_, p, err := events.DecodeRunCancelled(cancelledFields(`"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b"`))
	require.NoError(t, err)
	assert.NotNil(t, p.Attempts)
	assert.Empty(t, p.Attempts)
}

// cancelledFields is a run.cancelled:v1 entry of run-controller with the given payload
// members.
func cancelledFields(payload string) map[string]string {
	return map[string]string{
		"event_id":       "e",
		"tenant_id":      "default",
		"occurred_at":    "2026-10-23T09:14:07.000000Z",
		"producer":       "run-controller",
		"schema_version": "1",
		"payload":        "{" + payload + "}",
	}
}
