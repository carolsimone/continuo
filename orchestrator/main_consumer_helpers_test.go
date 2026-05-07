package main

import (
	"testing"

	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
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
	cmd, err := parseSchedulerStartedMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, runnerID, cmd.ScheduleID)
	assert.Equal(t, "test_schedule", cmd.ScheduleName)
	assert.Equal(t, "cron", cmd.Kind)
	assert.Nil(t, cmd.SourceRunID)
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
	cmd, err := parseSchedulerStartedMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, "rerun", cmd.Kind)
	require.NotNil(t, cmd.SourceRunID)
	assert.Equal(t, sourceID, *cmd.SourceRunID)
}

func TestParseSchedulerStartedMessage_EmptySourceRunIDIsNil(t *testing.T) {
	runnerID := uuid.New()
	msg := map[string]interface{}{
		"runner_id":     runnerID.String(),
		"schedule_name": "test_schedule",
		"kind":          "cron",
		"source_run_id": "",
	}
	cmd, err := parseSchedulerStartedMessage(msg)
	require.NoError(t, err)
	assert.Nil(t, cmd.SourceRunID)
}

func TestParseSchedulerStartedMessage_InvalidRunnerID(t *testing.T) {
	msg := map[string]interface{}{
		"runner_id":     "not-a-uuid",
		"schedule_name": "test_schedule",
	}
	_, err := parseSchedulerStartedMessage(msg)
	require.Error(t, err)
}

var _ = domainCmd.SchedulerStartedCmd{} // anchor import
