package redis_test

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSchedulerStartedEvent_Defaults(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
	}
	evt, err := redis.ParseSchedulerStartedEvent(msg)
	require.NoError(t, err)
	assert.Equal(t, runnerID, evt.ScheduleID)
	assert.Equal(t, "test_schedule", evt.ScheduleName)
	assert.Equal(t, "cron", evt.Kind)
	assert.Nil(t, evt.SourceRunID)
	// A message without initiated_by (or with it empty) is recorded as the
	// system sentinel rather than a blank provenance.
	assert.Equal(t, "system", evt.InitiatedBy)
}

func TestParseSchedulerStartedEvent_CarriesInitiatedBy(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"initiated_by":  "okta|alice",
	}
	evt, err := redis.ParseSchedulerStartedEvent(msg)
	require.NoError(t, err)
	assert.Equal(t, "okta|alice", evt.InitiatedBy)
}

func TestParseSchedulerStartedEvent_KindAndSource(t *testing.T) {
	runnerID := uuid.New()
	sourceID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"kind":          "rerun",
		"source_run_id": sourceID.String(),
	}
	evt, err := redis.ParseSchedulerStartedEvent(msg)
	require.NoError(t, err)
	assert.Equal(t, "rerun", evt.Kind)
	require.NotNil(t, evt.SourceRunID)
	assert.Equal(t, sourceID, *evt.SourceRunID)
}

func TestParseSchedulerStartedEvent_EmptySourceRunIDIsNil(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"kind":          "cron",
		"source_run_id": "",
	}
	evt, err := redis.ParseSchedulerStartedEvent(msg)
	require.NoError(t, err)
	assert.Nil(t, evt.SourceRunID)
}

func TestParseSchedulerStartedEvent_CarriesOperation(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"operation":     "test",
	}
	evt, err := redis.ParseSchedulerStartedEvent(msg)
	require.NoError(t, err)
	assert.Equal(t, "test", evt.Operation)
}

func TestParseSchedulerStartedEvent_MissingOperationDefaultsEmpty(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
	}
	evt, err := redis.ParseSchedulerStartedEvent(msg)
	require.NoError(t, err)
	assert.Equal(t, "", evt.Operation)
}

func TestParseSchedulerStartedEvent_InvalidRunnerID(t *testing.T) {
	msg := map[string]interface{}{
		"runner_id":     "not-a-uuid",
		"schedule_name": "test_schedule",
	}
	_, err := redis.ParseSchedulerStartedEvent(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
