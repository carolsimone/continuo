package main

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSchedulerStartedMessage_Defaults(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
	}
	evt, err := parseSchedulerStartedMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, runnerID, evt.ScheduleID)
	assert.Equal(t, "test_schedule", evt.ScheduleName)
	assert.Equal(t, "cron", evt.Kind)
	assert.Nil(t, evt.SourceRunID)
}

func TestParseSchedulerStartedMessage_KindAndSource(t *testing.T) {
	runnerID := uuid.New()
	sourceID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"kind":          "rerun",
		"source_run_id": sourceID.String(),
	}
	evt, err := parseSchedulerStartedMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, "rerun", evt.Kind)
	require.NotNil(t, evt.SourceRunID)
	assert.Equal(t, sourceID, *evt.SourceRunID)
}

func TestParseSchedulerStartedMessage_EmptySourceRunIDIsNil(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"kind":          "cron",
		"source_run_id": "",
	}
	evt, err := parseSchedulerStartedMessage(msg)
	require.NoError(t, err)
	assert.Nil(t, evt.SourceRunID)
}

func TestParseSchedulerStartedMessage_InvalidRunnerID(t *testing.T) {
	msg := map[string]interface{}{
		"runner_id":     "not-a-uuid",
		"schedule_name": "test_schedule",
	}
	_, err := parseSchedulerStartedMessage(msg)
	require.Error(t, err)
}

var _ = domain.SchedulerStarted{} // anchor import
