package redis

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureCancel reads a golden run.cancelled:v1 entry as XREADGROUP returns
// it: every field a string, the payload as JSON text.
func fixtureCancel(t *testing.T, name string) goredis.XMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", name)) //nolint:gosec // G304: a fixed fixture path
	require.NoError(t, err)
	var fx struct {
		Fields map[string]json.RawMessage `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(raw, &fx))
	values := map[string]any{}
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

// The first send of a cancel names no attempt; a re-send names each attempt
// run-controller waits for, with its Job.
func TestParseRunCancelled_ReadsTheGoldenFixtures(t *testing.T) {
	runID := uuid.MustParse("7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b")
	for name, want := range map[string]events.RunCancelled{
		"run_cancelled_v1.json": {TenantID: "default", RunID: runID, CancelledBy: "alice@example.com", Reason: "bad upstream data",
			Attempts: []events.AttemptRef{}},
		"run_cancelled_v1_attempts.json": {TenantID: "default", RunID: runID, CancelledBy: "alice@example.com", Reason: "bad upstream data",
			Attempts: []events.AttemptRef{
				{TaskID: "3e41c7d2-5a98-4b36-8f10-2d7e9a4c6b15", Attempt: 0, JobName: "service-1-e2e-schema-table-a-7f3e1a2b"},
				{TaskID: "9b07e5a3-1c64-4d82-b9f7-3a5c8e0d2f46", Attempt: 1, JobName: "service-1-e2e-schema-table-b-7f3e1a2b-r1"},
			}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ParseRunCancelled(fixtureCancel(t, name))
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

// An entry whose run cannot be read is dead-lettered.
func TestParseRunCancelled_AnEntryWithoutItsRunIsPermanent(t *testing.T) {
	msg := fixtureCancel(t, "run_cancelled_v1.json")
	msg.Values["payload"] = `{"cancelled_by":"alice@example.com","attempts":[]}`

	_, err := ParseRunCancelled(msg)

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
}

// An entry that names no tenant is dead-lettered: the reports of the attempts
// it names could not be published under one.
func TestParseRunCancelled_AnEntryWithoutATenantIsPermanent(t *testing.T) {
	msg := fixtureCancel(t, "run_cancelled_v1.json")
	msg.Values["tenant_id"] = ""

	_, err := ParseRunCancelled(msg)

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
}
