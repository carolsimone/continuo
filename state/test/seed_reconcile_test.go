package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	stateredis "github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	svcports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/seedreconcile"
	"github.com/carolsimone/continuo/state/service/uow"
)

// firstPromoteSeedRunID is the id a release's first promote-seed run has
// always carried: UUIDv5 of the release under the fixed promote-seed
// namespace.
func firstPromoteSeedRunID(releaseID string) uuid.UUID {
	return uuid.NewSHA1(uuid.MustParse("3e7b1a2f-8c4d-4e9a-b5f0-1d2c3a4e5f60"), []byte("schedule:"+releaseID))
}

// reconcileHarness runs the seed reconcile, alone and inside state's
// release.promoted:v2 binding, against one tenant of its own.
type reconcileHarness struct {
	db         *sqlx.DB
	tenant     string
	factory    func() uow.UnitOfWork
	reconciler *seedreconcile.Reconciler
	artifacts  *memoryArtifacts
	binding    pkgredis.MessageHandler
}

func newReconcileHarness(db *sqlx.DB) *reconcileHarness {
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	execRepo := postgres.NewTaskExecutionRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, execRepo, catalogRepo, svcports.SystemClock{}, logger)
	}
	tenant := "t-" + uuid.NewString()
	reconciler := seedreconcile.New(factory, false, tenant, logger)
	artifacts := &memoryArtifacts{byRef: map[string]svcports.TopologyArtifact{}}
	handler := svchandlers.NewReleasePromotedHandler(artifacts, reconciler, logger)
	return &reconcileHarness{
		db: db, tenant: tenant, factory: factory, reconciler: reconciler, artifacts: artifacts,
		binding: stateredis.NewReleasePromotedBinding(factory, handler, logger),
	}
}

// liveRelease stores releaseID for the harness's tenant under seq and makes it
// live, in one committed transaction, without running the seed reconcile.
func (h *reconcileHarness) liveRelease(t *testing.T, releaseID string, seq int64, specs ...topology.NodeSpec) {
	t.Helper()
	ctx := context.Background()
	rel, err := topology.NewRelease(h.tenant, releaseID, seq,
		"s3://continuo/tenants/"+h.tenant+"/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID, specs)
	require.NoError(t, err)
	u := h.factory()
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback() //nolint:errcheck
	_, err = u.Topology().SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, u.Topology().SetLivePointer(ctx, h.tenant, topology.Pointer{ReleaseID: releaseID, PromotionSeq: seq}, time.Now()))
	require.NoError(t, u.Commit())
}

// loaded records schema.table as last loaded at hash.
func (h *reconcileHarness) loaded(t *testing.T, schema, table, hash string) {
	t.Helper()
	require.NoError(t, postgres.NewMaterializationRepository(h.db, nil).RecordBuilt(context.Background(), h.tenant, seeds.Built{
		Key: topology.KeyOf(schema, table), ContentHash: hash, ImageTag: "service-1:v0", ReleaseID: "rel-before",
		At: time.Now(), Source: seeds.SourceAdopted,
	}))
}

// promote delivers one release.promoted:v2 entry for the harness's tenant to
// state's binding, with the artifact it names held in memory.
func (h *reconcileHarness) promote(t *testing.T, releaseID string, seq int64, bootstrap bool, changed []string, nodes ...topology.NodeSpec) {
	t.Helper()
	uri := "s3://continuo/tenants/" + h.tenant + "/topologies/" + releaseID + "/topology.json.gz"
	sha := "sha-" + releaseID
	h.artifacts.byRef[uri+"|"+sha] = svcports.TopologyArtifact{TenantID: h.tenant, ReleaseID: releaseID, Nodes: nodes}
	now := time.Now().UTC()
	fields, err := pkgevents.ReleasePromotedFields(h.tenant, "release-controller", now, pkgevents.ReleasePromoted{
		ReleaseID: releaseID, PromotedAt: now, PromotionSeq: seq, TopologyURI: uri, TopologySHA256: sha,
		ChangedNodeIDs: changed, Bootstrap: bootstrap,
	})
	require.NoError(t, err)
	msgID := fmt.Sprintf("%d-0", time.Now().UnixNano())
	require.NoError(t, h.binding(context.Background(), goredis.XMessage{ID: msgID, Values: fields}))
}

// promoteSeedRuns lists the promote-seed runs that name releaseID.
func (h *reconcileHarness) promoteSeedRuns(t *testing.T, releaseID string) []uuid.UUID {
	t.Helper()
	var ids []uuid.UUID
	require.NoError(t, h.db.Select(&ids,
		`SELECT schedule_id FROM scheduler_tracker WHERE kind = 'promote_seed' AND release_id = $1 ORDER BY created_at`, releaseID))
	return ids
}

// materializationRow is one node_materializations row.
type materializationRow struct {
	ContentHash        string        `db:"content_hash"`
	ReleaseID          string        `db:"release_id"`
	Source             *string       `db:"source"`
	AttemptContentHash string        `db:"attempt_content_hash"`
	AttemptRunID       uuid.NullUUID `db:"attempt_run_id"`
}

func (h *reconcileHarness) materialization(t *testing.T, key string) materializationRow {
	t.Helper()
	var row materializationRow
	require.NoError(t, h.db.Get(&row, `
		SELECT content_hash, release_id, source, attempt_content_hash, attempt_run_id
		FROM node_materializations WHERE tenant_id = $1 AND node_key = $2`, h.tenant, key))
	return row
}

// TestSeedReconcile drives the seed reconcile against a real Postgres: the run
// and build request a changed seed gets, the passes that must create nothing,
// two passes racing, and the first promotion's adoption through the
// release.promoted:v2 binding. Each subtest uses its own tenant and releases.
func TestSeedReconcile(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("a changed seed gets one run, a build request and a trigger, then waits", func(t *testing.T) {
		h := newReconcileHarness(db)
		releaseID := "rel-" + uuid.NewString()[:8]
		h.liveRelease(t, releaseID, 7, promotedNode("raw", "orders", "dbt-seed"), promotedNode("stg", "orders", "dbt-model", "raw.orders"))
		h.loaded(t, "raw", "orders", "sha256:previous")

		require.NoError(t, h.reconciler.RunOnce(ctx))

		runID := firstPromoteSeedRunID(releaseID)
		require.Equal(t, []uuid.UUID{runID}, h.promoteSeedRuns(t, releaseID), "the release's first run keeps its id")
		var runRow struct {
			Name   string `db:"schedule_name"`
			Status string `db:"status"`
		}
		require.NoError(t, db.Get(&runRow, `SELECT schedule_name, status FROM scheduler_tracker WHERE schedule_id = $1`, runID))
		assert.Equal(t, "promote-seed-"+strings.ReplaceAll(runID.String(), "-", "")[:8], runRow.Name)
		assert.Equal(t, "running", runRow.Status)

		orders := h.materialization(t, "raw.orders")
		assert.Equal(t, "sha256:previous", orders.ContentHash, "a request leaves the loaded version alone")
		assert.Equal(t, "sha256:orders", orders.AttemptContentHash)
		assert.Equal(t, uuid.NullUUID{UUID: runID, Valid: true}, orders.AttemptRunID)

		var outbox struct {
			StreamName string `db:"stream_name"`
			Payload    []byte `db:"payload"`
		}
		require.NoError(t, db.Get(&outbox, `SELECT stream_name, payload FROM state_outbox WHERE aggregate_id = $1`, runID))
		assert.Equal(t, streams.TriggerPromotedSeedsV1, outbox.StreamName)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(outbox.Payload, &payload))
		assert.Equal(t, releaseID, payload["release_id"])
		assert.Equal(t, "7", payload["promotion_seq"])
		assert.Equal(t, []any{map[string]any{
			"service_name": "service-1", "schema_name": "raw", "table_name": "orders",
			"node_type": "dbt-seed", "image_tag": "service-1:v1",
		}}, payload["nodes"])

		require.NoError(t, h.reconciler.RunOnce(ctx))
		assert.Len(t, h.promoteSeedRuns(t, releaseID), 1, "a second pass while the run is running creates nothing")
		assert.Equal(t, 1, countOf(t, db, `SELECT count(*) FROM state_outbox WHERE stream_name = $1 AND payload->>'release_id' = $2`,
			streams.TriggerPromotedSeedsV1, releaseID))

		_, err := db.Exec(`UPDATE scheduler_tracker SET status = 'failed', completed_at = now() WHERE schedule_id = $1`, runID)
		require.NoError(t, err)
		require.NoError(t, h.reconciler.RunOnce(ctx))
		assert.Len(t, h.promoteSeedRuns(t, releaseID), 1, "a failed load of the live content is not retried")
	})

	t.Run("two passes racing create one run", func(t *testing.T) {
		h := newReconcileHarness(db)
		releaseID := "rel-" + uuid.NewString()[:8]
		h.liveRelease(t, releaseID, 3, promotedNode("raw", "orders", "dbt-seed"), promotedNode("raw", "users", "dbt-seed"))

		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = h.reconciler.RunOnce(ctx)
			}(i)
		}
		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		assert.Equal(t, []uuid.UUID{firstPromoteSeedRunID(releaseID)}, h.promoteSeedRuns(t, releaseID))
		assert.Equal(t, 1, countOf(t, db, `SELECT count(*) FROM state_outbox WHERE stream_name = $1 AND payload->>'release_id' = $2`,
			streams.TriggerPromotedSeedsV1, releaseID))
	})

	t.Run("a first promotion adopts the seeds it did not change and builds the one it did", func(t *testing.T) {
		h := newReconcileHarness(db)
		releaseID := "rel-" + uuid.NewString()[:8]

		h.promote(t, releaseID, 2, false, []string{"raw.orders"},
			promotedNode("raw", "orders", "dbt-seed"), promotedNode("raw", "users", "dbt-seed"))

		users := h.materialization(t, "raw.users")
		assert.Equal(t, "sha256:users", users.ContentHash)
		require.NotNil(t, users.Source)
		assert.Equal(t, "adopted", *users.Source)
		assert.Equal(t, releaseID, users.ReleaseID)
		orders := h.materialization(t, "raw.orders")
		assert.Empty(t, orders.ContentHash)
		assert.Equal(t, "sha256:orders", orders.AttemptContentHash)
		assert.Equal(t, []uuid.UUID{firstPromoteSeedRunID(releaseID)}, h.promoteSeedRuns(t, releaseID))
	})

	t.Run("a first promotion whose release already has its first run adopts every seed", func(t *testing.T) {
		h := newReconcileHarness(db)
		releaseID := "rel-" + uuid.NewString()[:8]
		existing := firstPromoteSeedRunID(releaseID)
		_, err := db.Exec(`
			INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, initialization_status, kind)
			VALUES ($1, $2, 'succeeded', now(), 'completed', 'promote_seed')`,
			existing, "promote-seed-"+strings.ReplaceAll(existing.String(), "-", "")[:8])
		require.NoError(t, err)

		h.promote(t, releaseID, 2, false, []string{"raw.orders"},
			promotedNode("raw", "orders", "dbt-seed"), promotedNode("raw", "users", "dbt-seed"))

		assert.Equal(t, "sha256:orders", h.materialization(t, "raw.orders").ContentHash)
		assert.Equal(t, "sha256:users", h.materialization(t, "raw.users").ContentHash)
		assert.Empty(t, h.promoteSeedRuns(t, releaseID), "nothing is loaded again")
	})

	t.Run("a bootstrap release adopts nothing and builds every seed with a content hash", func(t *testing.T) {
		h := newReconcileHarness(db)
		releaseID := "rel-" + uuid.NewString()[:8]
		blank := promotedNode("raw", "blank", "dbt-seed")
		blank.ContentHash = ""

		h.promote(t, releaseID, 1, true, nil,
			promotedNode("raw", "orders", "dbt-seed"), promotedNode("raw", "users", "dbt-seed"), blank)

		runs := h.promoteSeedRuns(t, releaseID)
		require.Len(t, runs, 1)
		assert.Equal(t, 2, countOf(t, db, `
			SELECT count(*) FROM node_materializations
			WHERE tenant_id = $1 AND attempt_run_id = $2 AND content_hash = ''`, h.tenant, runs[0]))
		assert.Zero(t, countOf(t, db, `SELECT count(*) FROM node_materializations WHERE tenant_id = $1 AND node_key = 'raw.blank'`, h.tenant))
	})
}

// seedAt is the dbt seed raw.<table> carrying content hash hash.
func seedAt(table, hash string) topology.NodeSpec {
	n := promotedNode("raw", table, "dbt-seed")
	n.ContentHash = hash
	return n
}

// promotedSeedNodes lists the tables a release's trigger.promoted_seeds:v1 row
// asks to load.
func (h *reconcileHarness) promotedSeedNodes(t *testing.T, runID uuid.UUID) []string {
	t.Helper()
	var payload []byte
	require.NoError(t, h.db.Get(&payload, `SELECT payload FROM state_outbox WHERE aggregate_id = $1`, runID))
	var body struct {
		Nodes []struct {
			TableName string `json:"table_name"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(payload, &body))
	tables := make([]string, 0, len(body.Nodes))
	for _, n := range body.Nodes {
		tables = append(tables, n.TableName)
	}
	return tables
}

// TestSeedReconcile_ReplayOfSeveralRetainedPromotions delivers three retained
// promotions oldest first, as an upgraded state does when its consumer group
// starts at the beginning of the stream. orders changes in every release and
// users changes once. Each seed is built once during the replay, however many
// releases it changes in: a build in flight holds back a later one, so orders
// waits on the build of the first release. The seed left behind is built at its
// live content by the first pass after that build ends.
func TestSeedReconcile_ReplayOfSeveralRetainedPromotions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	h := newReconcileHarness(db)
	relA, relB, relC := "rel-a-"+uuid.NewString()[:8], "rel-b-"+uuid.NewString()[:8], "rel-c-"+uuid.NewString()[:8]

	h.promote(t, relA, 1, false, []string{"raw.orders"}, seedAt("orders", "sha256:orders-a"), seedAt("users", "sha256:users-a"))
	h.promote(t, relB, 2, false, []string{"raw.orders"}, seedAt("orders", "sha256:orders-b"), seedAt("users", "sha256:users-a"))
	h.promote(t, relC, 3, false, []string{"raw.orders", "raw.users"}, seedAt("orders", "sha256:orders-c"), seedAt("users", "sha256:users-c"))

	require.Equal(t, topology.Pointer{ReleaseID: relC, PromotionSeq: 3}, livePointerOf(t, db, h.tenant))
	runA, runC := firstPromoteSeedRunID(relA), firstPromoteSeedRunID(relC)
	assert.Equal(t, []uuid.UUID{runA}, h.promoteSeedRuns(t, relA), "the first release builds orders")
	assert.Empty(t, h.promoteSeedRuns(t, relB), "orders is already being built and users is as loaded")
	assert.Equal(t, []uuid.UUID{runC}, h.promoteSeedRuns(t, relC), "the third release builds users")
	assert.Equal(t, []string{"orders"}, h.promotedSeedNodes(t, runA))
	assert.Equal(t, []string{"users"}, h.promotedSeedNodes(t, runC))
	assert.Equal(t, 2, countOf(t, db, `SELECT count(*) FROM state_outbox WHERE stream_name = $1 AND aggregate_id IN ($2, $3)`,
		streams.TriggerPromotedSeedsV1, runA, runC), "one trigger per build")

	orders := h.materialization(t, "raw.orders")
	assert.Empty(t, orders.ContentHash, "nothing is recorded as loaded for orders yet")
	assert.Equal(t, "sha256:orders-a", orders.AttemptContentHash, "orders was requested once, at the release that began the replay")
	assert.Equal(t, uuid.NullUUID{UUID: runA, Valid: true}, orders.AttemptRunID)
	users := h.materialization(t, "raw.users")
	assert.Equal(t, "sha256:users-a", users.ContentHash, "users was adopted at the first release's content")
	assert.Equal(t, "adopted", *users.Source)
	assert.Equal(t, "sha256:users-c", users.AttemptContentHash)
	assert.Equal(t, uuid.NullUUID{UUID: runC, Valid: true}, users.AttemptRunID)

	require.NoError(t, h.reconciler.RunOnce(ctx))
	assert.Equal(t, 2, countOf(t, db, `SELECT count(*) FROM scheduler_tracker WHERE kind = 'promote_seed' AND release_id IN ($1, $2, $3)`,
		relA, relB, relC), "a pass while both builds run creates nothing")

	// The build of orders at the first release ends and its task records the
	// version it loaded, as a task's success does.
	_, err := db.Exec(`UPDATE scheduler_tracker SET status = 'succeeded', completed_at = now() WHERE schedule_id = $1`, runA)
	require.NoError(t, err)
	require.NoError(t, postgres.NewMaterializationRepository(db, nil).RecordBuilt(ctx, h.tenant, seeds.Built{
		Key: "raw.orders", ContentHash: "sha256:orders-a", ImageTag: "service-1:v1", ReleaseID: relA,
		RunID: runA, At: time.Now(), Source: seeds.SourceBuild,
	}))

	require.NoError(t, h.reconciler.RunOnce(ctx))

	followUp := seeds.RunID(relC, 2)
	assert.Equal(t, []uuid.UUID{runC, followUp}, h.promoteSeedRuns(t, relC), "the live release's second run loads the content orders is missing")
	assert.Equal(t, []string{"orders"}, h.promotedSeedNodes(t, followUp))
	orders = h.materialization(t, "raw.orders")
	assert.Equal(t, "sha256:orders-c", orders.AttemptContentHash, "orders is now requested at the live content")
	assert.Equal(t, uuid.NullUUID{UUID: followUp, Valid: true}, orders.AttemptRunID)
	assert.Equal(t, "sha256:users-c", h.materialization(t, "raw.users").AttemptContentHash, "users is still waiting on its own build")
	assert.Equal(t, 3, countOf(t, db, `SELECT count(*) FROM scheduler_tracker WHERE kind = 'promote_seed' AND release_id IN ($1, $2, $3)`,
		relA, relB, relC), "three builds in all: orders twice because it changed while loading, users once")
}
