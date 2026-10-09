package test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	stateredis "github.com/carolsimone/continuo/state/adapters/redis"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	svcports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// TestReleaseSeedsPendingBinding_SecondDeliveryForTheSameReleaseIsANoOp drives
// the release.seeds.pending:v1 binding twice for one release, as orchestrator
// does when it handles the same release.promoted:v2 twice (a redelivery or a
// dead-letter redrive): each delivery has its own stream message id and outbox
// entry id, so dedup does not catch the second. Both deliveries must commit,
// and the release must end up with one run and one trigger.promoted_seeds:v1
// outbox row.
func TestReleaseSeedsPendingBinding_SecondDeliveryForTheSameReleaseIsANoOp(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))

	schedulerRepo := postgres.NewSchedulerTrackerRepository(rawDB, logger)
	taskRepo := postgres.NewTaskTrackerRepository(rawDB, logger)
	execRepo := postgres.NewTaskExecutionRepository(rawDB, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(rawDB, logger)
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(rawDB, schedulerRepo, taskRepo, execRepo, catalogRepo, svcports.SystemClock{}, logger)
	}
	binding := stateredis.NewReleaseSeedsPendingBinding(factory, svchandlers.NewPromotedSeedsHandler(logger), logger)

	const releaseID = "rel-redelivered"
	payload := `{"release_id":"` + releaseID + `","nodes":[{"service_name":"core","schema_name":"analytics","table_name":"seed_users","node_type":"dbt-seed","image_tag":"v1"}]}`
	for i, messageID := range []string{"1759665600000-0", "1759665700000-0"} {
		err := binding(context.Background(), goredis.XMessage{
			ID: messageID,
			Values: map[string]interface{}{
				"payload":         payload,
				"outbox_entry_id": uuid.NewString(),
			},
		})
		require.NoError(t, err, "delivery %d", i+1)
	}

	runID := svchandlers.PromotedSeedsRunID(releaseID)
	var runs int
	require.NoError(t, rawDB.Get(&runs, `SELECT count(*) FROM scheduler_tracker WHERE schedule_id = $1`, runID))
	assert.Equal(t, 1, runs, "one promoted-seeds run per release")

	var outboxRows int
	require.NoError(t, rawDB.Get(&outboxRows, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, runID))
	assert.Equal(t, 1, outboxRows, "trigger.promoted_seeds:v1 is written once, by the first delivery")

	var dedupRows int
	require.NoError(t, rawDB.Get(&dedupRows, `SELECT count(*) FROM message_processing WHERE stream_name = $1`, streams.ReleaseSeedsPendingV1))
	assert.Equal(t, 2, dedupRows, "both deliveries commit their dedup row")
}
