package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	stateredis "github.com/carolsimone/continuo/state/adapters/redis"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	svcports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// TestScheduleCatalogBinding_OlderPromotionLeavesTheCatalogUnchanged drives the
// schedules.loaded:v1 binding with a newer promotion and then an older one, as
// orchestrator's outbox delivers them when the older release's row is retried
// past the newer: each has its own stream message id and outbox entry id, so
// dedup does not catch the older. The older must be acknowledged and change
// neither the catalog nor its recorded seq; the same seq again re-applies.
func TestScheduleCatalogBinding_OlderPromotionLeavesTheCatalogUnchanged(t *testing.T) {
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
	binding := stateredis.NewScheduleCatalogBinding(factory, svchandlers.NewScheduleCatalogHandler(logger), logger)

	deliver := func(messageID string, seq int64, names ...string) {
		t.Helper()
		body, err := json.Marshal(map[string]any{
			"event_id":         uuid.NewString(),
			"schedule_names":   names,
			"service_metadata": map[string]map[string]string{"core": {"image_tag": "v1"}},
			"promotion_seq":    seq,
		})
		require.NoError(t, err)
		require.NoError(t, binding(context.Background(), goredis.XMessage{
			ID: messageID,
			Values: map[string]interface{}{
				"payload":         string(body),
				"outbox_entry_id": uuid.NewString(),
			},
		}), "delivery %s must be acknowledged", messageID)
	}
	active := func() []string {
		var names []string
		require.NoError(t, rawDB.Select(&names,
			`SELECT schedule_name FROM schedule_catalog WHERE removed_at IS NULL ORDER BY schedule_name`))
		return names
	}
	recordedSeq := func() int64 {
		var seq int64
		require.NoError(t, rawDB.Get(&seq, `SELECT promotion_seq FROM schedule_catalog_state WHERE id = TRUE`))
		return seq
	}

	deliver("1759665600000-0", 5, "daily", "hourly")
	require.Equal(t, []string{"daily", "hourly"}, active())
	require.Equal(t, int64(5), recordedSeq())

	deliver("1759665700000-0", 4, "daily")
	assert.Equal(t, []string{"daily", "hourly"}, active(), "an older promotion must not roll the catalog back")
	assert.Equal(t, int64(5), recordedSeq())

	deliver("1759665800000-0", 5, "daily")
	assert.Equal(t, []string{"daily"}, active(), "the same promotion seq re-applies")
	assert.Equal(t, int64(5), recordedSeq())
}
