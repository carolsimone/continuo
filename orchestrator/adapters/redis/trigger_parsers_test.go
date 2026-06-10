package redis_test

import (
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRerun_Valid(t *testing.T) {
	cmd, err := redis.ParseRerun(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id":   "sched-1",
		"schedule_name": "daily",
		"source_run_id": "src-1",
	}})
	require.NoError(t, err)
	assert.Equal(t, "sched-1", cmd.RunID)
	assert.Equal(t, "daily", cmd.ScheduleName)
	assert.Equal(t, "src-1", cmd.SourceRunID)
}

func TestParseRerun_MissingFieldIsPermanent(t *testing.T) {
	for _, missing := range []string{"schedule_id", "schedule_name", "source_run_id"} {
		t.Run(missing, func(t *testing.T) {
			values := map[string]interface{}{
				"schedule_id":   "sched-1",
				"schedule_name": "daily",
				"source_run_id": "src-1",
			}
			delete(values, missing)
			_, err := redis.ParseRerun(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.True(t, errors.Is(err, events.ErrPermanent))
		})
	}
}

func TestParseRebase_Valid(t *testing.T) {
	cmd, err := redis.ParseRebase(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id":   "sched-1",
		"schedule_name": "daily",
		"source_run_id": "src-1",
	}})
	require.NoError(t, err)
	assert.Equal(t, "sched-1", cmd.RunID)
	assert.Equal(t, "daily", cmd.ScheduleName)
	assert.Equal(t, "src-1", cmd.SourceRunID)
}

func TestParseRebase_MissingFieldIsPermanent(t *testing.T) {
	_, err := redis.ParseRebase(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id":   "sched-1",
		"schedule_name": "daily",
	}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, events.ErrPermanent))
}

func singleNodeRunValues() map[string]interface{} {
	return map[string]interface{}{
		"schedule_id":     "sched-1",
		"schedule_name":   "single-node-run-abc",
		"schema_name":     "analytics",
		"table_name":      "orders",
		"service_name":    "service-1",
		"metadata_source": "latest",
	}
}

func TestParseSingleNodeRun_LatestValid(t *testing.T) {
	cmd, err := redis.ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: singleNodeRunValues()})
	require.NoError(t, err)
	assert.Equal(t, "sched-1", cmd.RunID)
	assert.Equal(t, "latest", cmd.MetadataSource)
	assert.Empty(t, cmd.SourceRunID)
}

func TestParseSingleNodeRun_SnapshotValid(t *testing.T) {
	values := singleNodeRunValues()
	values["metadata_source"] = "snapshot_of_run"
	values["source_run_id"] = "src-1"
	cmd, err := redis.ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: values})
	require.NoError(t, err)
	assert.Equal(t, "snapshot_of_run", cmd.MetadataSource)
	assert.Equal(t, "src-1", cmd.SourceRunID)
}

func TestParseSingleNodeRun_MissingNodeFieldIsPermanent(t *testing.T) {
	for _, missing := range []string{"schedule_id", "schedule_name", "schema_name", "table_name", "service_name"} {
		t.Run(missing, func(t *testing.T) {
			values := singleNodeRunValues()
			delete(values, missing)
			_, err := redis.ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.True(t, errors.Is(err, events.ErrPermanent))
		})
	}
}

func TestParseSingleNodeRun_CrossFieldViolationsArePermanent(t *testing.T) {
	t.Run("latest_with_source", func(t *testing.T) {
		values := singleNodeRunValues()
		values["source_run_id"] = "src-1"
		_, err := redis.ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: values})
		require.Error(t, err)
		assert.True(t, errors.Is(err, events.ErrPermanent))
	})
	t.Run("snapshot_without_source", func(t *testing.T) {
		values := singleNodeRunValues()
		values["metadata_source"] = "snapshot_of_run"
		_, err := redis.ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: values})
		require.Error(t, err)
		assert.True(t, errors.Is(err, events.ErrPermanent))
	})
	t.Run("invalid_metadata_source", func(t *testing.T) {
		values := singleNodeRunValues()
		values["metadata_source"] = "bogus"
		_, err := redis.ParseSingleNodeRun(goredis.XMessage{ID: "1-0", Values: values})
		require.Error(t, err)
		assert.True(t, errors.Is(err, events.ErrPermanent))
	})
}
