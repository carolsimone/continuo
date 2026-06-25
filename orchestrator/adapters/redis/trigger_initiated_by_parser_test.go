package redis

import (
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The trigger parsers carry the initiating-user provenance through to the
// handler input, defaulting to the "system" sentinel when the field is absent
// (messages from a state version predating provenance tracking).
func TestTriggerParsers_InitiatedBy(t *testing.T) {
	scheduleID := uuid.New().String()
	sourceRunID := uuid.New().String()

	t.Run("rerun present", func(t *testing.T) {
		in, err := ParseRerun(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
			"schedule_id":   scheduleID,
			"schedule_name": "daily",
			"source_run_id": sourceRunID,
			"initiated_by":  "okta|alice",
		}})
		require.NoError(t, err)
		assert.Equal(t, "okta|alice", in.InitiatedBy)
	})

	t.Run("rerun absent defaults to system", func(t *testing.T) {
		in, err := ParseRerun(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
			"schedule_id":   scheduleID,
			"schedule_name": "daily",
			"source_run_id": sourceRunID,
		}})
		require.NoError(t, err)
		assert.Equal(t, "system", in.InitiatedBy)
	})

	t.Run("rebase present", func(t *testing.T) {
		in, err := ParseRebase(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
			"schedule_id":   scheduleID,
			"schedule_name": "daily",
			"source_run_id": sourceRunID,
			"initiated_by":  "okta|bob",
		}})
		require.NoError(t, err)
		assert.Equal(t, "okta|bob", in.InitiatedBy)
	})

	t.Run("single node present", func(t *testing.T) {
		in, err := ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
			"schedule_id":     scheduleID,
			"schedule_name":   "single-node-run-abcd1234",
			"service_name":    "svc",
			"schema_name":     "sch",
			"table_name":      "tbl",
			"metadata_source": "latest",
			"initiated_by":    "okta|carol",
		}})
		require.NoError(t, err)
		assert.Equal(t, "okta|carol", in.InitiatedBy)
	})

	t.Run("single node absent defaults to system", func(t *testing.T) {
		in, err := ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
			"schedule_id":     scheduleID,
			"schedule_name":   "single-node-run-abcd1234",
			"service_name":    "svc",
			"schema_name":     "sch",
			"table_name":      "tbl",
			"metadata_source": "latest",
		}})
		require.NoError(t, err)
		assert.Equal(t, "system", in.InitiatedBy)
	})
}
