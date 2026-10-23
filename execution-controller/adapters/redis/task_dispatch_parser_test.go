package redis

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureDispatch reads the golden task.dispatch:v1 entry as XREADGROUP
// returns it: every field a string, the payload as JSON text.
func fixtureDispatch(t *testing.T) goredis.XMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", "task_dispatch_v1.json"))
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

func TestParseTaskDispatch_ReadsTheGoldenEntry(t *testing.T) {
	evt, err := ParseTaskDispatch(fixtureDispatch(t))
	require.NoError(t, err)
	assert.Equal(t, uuid.MustParse("7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b"), evt.RunID)
	assert.Equal(t, uuid.MustParse("3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"), evt.TaskID)
	assert.Equal(t, 1, evt.Attempt)
	assert.Equal(t, pkgmodel.AttemptStatusDispatched, evt.KnownStatus)
	assert.Equal(t, "service-1-e2e-schema-ftable-e-7f3e1a2b-r1", evt.JobName)
	assert.Equal(t, int64(86400), evt.TimeoutSeconds)
	assert.Equal(t, "dbt-model", evt.NodeType)
}

// An entry whose attempt cannot be read is dead-lettered.
func TestParseTaskDispatch_AnEntryWithoutItsAttemptIsPermanent(t *testing.T) {
	msg := fixtureDispatch(t)
	msg.Values["payload"] = `{"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"}`

	_, err := ParseTaskDispatch(msg)

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
}
