package handlers

import (
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDomainToProtoScheduler_PopulatesStartedAt confirms the tracker→proto
// mapper used by ListAllSchedules/GetScheduler surfaces started_at to the UI
// and CLI once it is persisted.
func TestDomainToProtoScheduler_PopulatesStartedAt(t *testing.T) {
	started := time.Now()
	tracker := &postgres.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "s",
		Status:               run.SchedulerStatusRunning,
		CreatedAt:            started.Add(-time.Minute),
		StartedAt:            &started,
		InitializationStatus: "completed",
	}

	got := domainToProtoScheduler(tracker)
	require.NotNil(t, got.StartedAt, "proto StartedAt must be populated from tracker.StartedAt")
	assert.WithinDuration(t, started, got.StartedAt.AsTime(), time.Second)
}

// TestRunToProto_PopulatesStartedAt confirms the Run-aggregate→proto mapper
// (used by the cancel/single-node responses) surfaces started_at.
func TestRunToProto_PopulatesStartedAt(t *testing.T) {
	started := time.Now()
	created := started.Add(-time.Minute)
	rn := run.HydrateRun(
		uuid.New(),
		"s",
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		run.KindCron,
		nil,
		created,
		&started, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Int32: 1, Valid: true},
		0,
		map[string]run.ServiceMetadata{},
	)

	got := runToProto(rn)
	require.NotNil(t, got.StartedAt, "proto StartedAt must be populated from Run.StartedAt()")
	assert.WithinDuration(t, started, got.StartedAt.AsTime(), time.Second)
}
