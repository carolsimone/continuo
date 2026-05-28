package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	pginfra "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wipeReleasePromotedFixtures removes any :Table nodes and the
// :Meta {key:'current_release'} singleton so each test starts clean.
func wipeReleasePromotedFixtures(t *testing.T, client neo4jinfra.Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	_, err := s.Run(ctx, `MATCH (n:Table) DETACH DELETE n`, nil)
	require.NoError(t, err)
	_, err = s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) DELETE m`, nil)
	require.NoError(t, err)
}

// TestReleasePromotedConsumer_HappyPath_E2E exercises the full handler chain
// against real Neo4j and Postgres:
//   - :Table nodes and a :DEPENDS_ON edge are created.
//   - :Meta {key:'current_release'} is set to the release_id.
//   - A schedules.loaded:v1 outbox row is written with a valid payload shape.
//   - A dedup row is written to message_processing.
//
// The test skips automatically when Neo4j or Postgres are unavailable
// (infrastructure injected via environment variables, defaulting to docker
// compose addresses).
func TestReleasePromotedConsumer_HappyPath_E2E(t *testing.T) {
	ctx := context.Background()
	client := newCommandTestNeo4jClient(t)
	pgDB := newCommandTestDB(t)

	wipeReleasePromotedFixtures(t, client)
	t.Cleanup(func() {
		wipeReleasePromotedFixtures(t, client)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM orchestrator_outbox WHERE event_type = 'release_promoted'`)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM message_processing WHERE stream_name = $1`, streams.ReleasePromotedV1)
	})

	repo := neo4jinfra.NewReleasePromotionRepository(client, newTestLogger())
	handler := handlers.NewReleasePromotedHandler(
		pginfra.NewPostgresUnitOfWork(pgDB, newTestLogger()),
		repo,
		newTestLogger(),
	)

	releaseID := "rA-" + uuid.New().String()[:8]
	msgID := "msg-int-happy-" + uuid.New().String()[:8]

	cmd := domainModel.PromoteReleaseInput{
		ReleaseID: releaseID,
		Topology: []domainEvent.ReleasePromotedNode{
			{
				UniqueID:          "service-a.public.table_a",
				SchemaName:        "public",
				TableName:         "table_a",
				ServiceName:       "service-a",
				ImageTag:          "tag-a",
				Schedule:          "daily",
				UpstreamUniqueIDs: []string{},
			},
			{
				UniqueID:          "service-b.public.table_b",
				SchemaName:        "public",
				TableName:         "table_b",
				ServiceName:       "service-b",
				ImageTag:          "tag-b",
				Schedule:          "daily",
				UpstreamUniqueIDs: []string{"service-a.public.table_a"},
			},
		},
		ImageTags: map[string]string{
			"service-a": "tag-a",
			"service-b": "tag-b",
		},
	}

	require.NoError(t, handler.Handle(ctx, msgID, cmd))

	// ── Neo4j assertions ──────────────────────────────────────────────────────

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	// Two :Table nodes with the expected properties.
	res, err := s.Run(ctx, `MATCH (t:Table) RETURN t.unique_id AS uid, t.release_id AS rid ORDER BY uid`, nil)
	require.NoError(t, err)
	var uids []string
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		rid, _ := res.Record().Get("rid")
		uids = append(uids, uid.(string))
		assert.Equal(t, releaseID, rid, "release_id property must match the promoted release")
	}
	assert.Equal(t, []string{"service-a.public.table_a", "service-b.public.table_b"}, uids)

	// One :DEPENDS_ON edge from b to a (b's upstream is a).
	res, err = s.Run(ctx, `MATCH (a:Table)-[:DEPENDS_ON]->(b:Table) RETURN a.unique_id AS au, b.unique_id AS bu`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx), "exactly one DEPENDS_ON edge must exist")
	au, _ := res.Record().Get("au")
	bu, _ := res.Record().Get("bu")
	assert.Equal(t, "service-b.public.table_b", au)
	assert.Equal(t, "service-a.public.table_a", bu)

	// :Meta {key:'current_release'} reflects the promoted release_id.
	res, err = s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS rid`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx), ":Meta{key:'current_release'} must exist after promotion")
	metaRID, _ := res.Record().Get("rid")
	assert.Equal(t, releaseID, metaRID)

	// ── Postgres outbox assertions ────────────────────────────────────────────

	var outboxStreamName string
	var outboxPayload []byte
	err = pgDB.QueryRowContext(ctx,
		`SELECT stream_name, payload FROM orchestrator_outbox
		 WHERE event_type = 'release_promoted'
		 ORDER BY created_at DESC LIMIT 1`,
	).Scan(&outboxStreamName, &outboxPayload)
	require.NoError(t, err, "one schedules.loaded:v1 outbox row must exist")
	assert.Equal(t, streams.SchedulesLoadedV1, outboxStreamName)

	// Payload shape check: event_id + schedule_names + service_metadata.
	var outboxGot map[string]interface{}
	require.NoError(t, json.Unmarshal(outboxPayload, &outboxGot))
	assert.NotEmpty(t, outboxGot["event_id"], "event_id must be present and non-empty")
	assert.Equal(t, []interface{}{"daily"}, outboxGot["schedule_names"])
	sm, ok := outboxGot["service_metadata"].(map[string]interface{})
	require.True(t, ok, "service_metadata must be a JSON object")
	smA := sm["service-a"].(map[string]interface{})
	assert.Equal(t, releaseID, smA["manifest_version"])
	assert.Equal(t, "tag-a", smA["image_tag"])

	// ── Postgres dedup assertions ─────────────────────────────────────────────

	var dedupCount int
	require.NoError(t, pgDB.QueryRowContext(ctx,
		`SELECT count(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		msgID, streams.ReleasePromotedV1,
	).Scan(&dedupCount))
	assert.Equal(t, 1, dedupCount, "exactly one dedup row must be written for the message")
}

// TestReleasePromotedConsumer_Idempotent_E2E delivers the same logical
// release_id twice via two different Redis message IDs. The expected outcome is:
//   - Neo4j has no duplicates (unchanged after the second call).
//   - Postgres outbox has exactly 1 row: the second call's handler sees
//     changed=false from the repository (release_id already in :Meta) and
//     short-circuits before writing the outbox.
//   - Postgres dedup has 2 rows, one per message_id.
func TestReleasePromotedConsumer_Idempotent_E2E(t *testing.T) {
	ctx := context.Background()
	client := newCommandTestNeo4jClient(t)
	pgDB := newCommandTestDB(t)

	wipeReleasePromotedFixtures(t, client)
	t.Cleanup(func() {
		wipeReleasePromotedFixtures(t, client)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM orchestrator_outbox WHERE event_type = 'release_promoted'`)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM message_processing WHERE stream_name = $1`, streams.ReleasePromotedV1)
	})

	repo := neo4jinfra.NewReleasePromotionRepository(client, newTestLogger())
	msgID1 := "msg-idem-1-" + uuid.New().String()[:8]
	msgID2 := "msg-idem-2-" + uuid.New().String()[:8]
	releaseID := "rA-idem-" + uuid.New().String()[:8]

	cmd := domainModel.PromoteReleaseInput{
		ReleaseID: releaseID,
		Topology: []domainEvent.ReleasePromotedNode{
			{
				UniqueID:          "svc.public.t",
				SchemaName:        "public",
				TableName:         "t",
				ServiceName:       "svc",
				ImageTag:          "tag-1",
				Schedule:          "hourly",
				UpstreamUniqueIDs: []string{},
			},
		},
		ImageTags: map[string]string{"svc": "tag-1"},
	}

	// First delivery.
	handler1 := handlers.NewReleasePromotedHandler(
		pginfra.NewPostgresUnitOfWork(pgDB, newTestLogger()),
		repo,
		newTestLogger(),
	)
	require.NoError(t, handler1.Handle(ctx, msgID1, cmd))

	// Second delivery — same release_id, different message ID.
	handler2 := handlers.NewReleasePromotedHandler(
		pginfra.NewPostgresUnitOfWork(pgDB, newTestLogger()),
		repo,
		newTestLogger(),
	)
	require.NoError(t, handler2.Handle(ctx, msgID2, cmd))

	// ── Neo4j: no duplicates ──────────────────────────────────────────────────

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	res, err := s.Run(ctx, `MATCH (t:Table) RETURN count(t) AS n`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	n, _ := res.Record().Get("n")
	assert.Equal(t, int64(1), n, "second promotion with same release_id must not create duplicate nodes")

	// ── Outbox: exactly 1 row (second short-circuits at repo idempotency) ─────

	var outboxCount int
	require.NoError(t, pgDB.QueryRowContext(ctx,
		`SELECT count(*) FROM orchestrator_outbox WHERE event_type = 'release_promoted'`,
	).Scan(&outboxCount))
	assert.Equal(t, 1, outboxCount, "second message with same release_id must not produce a second outbox row")

	// ── Dedup: 2 rows (one per message_id) ───────────────────────────────────

	var dedupCount int
	require.NoError(t, pgDB.QueryRowContext(ctx,
		`SELECT count(*) FROM message_processing WHERE stream_name = $1 AND message_id IN ($2, $3)`,
		streams.ReleasePromotedV1, msgID1, msgID2,
	).Scan(&dedupCount))
	assert.Equal(t, 2, dedupCount, "each unique message_id must produce its own dedup row")
}

// TestReleasePromotedConsumer_TwoDifferentReleases_E2E promotes two successive
// releases (rA then rB) and verifies the TRUNCATE-AND-LOAD semantics:
//   - After rB, only rB's nodes exist in Neo4j (rA's nodes are gone).
//   - :Meta.release_id is "rB".
//   - Postgres outbox has 2 rows on schedules.loaded:v1 (one per promotion).
func TestReleasePromotedConsumer_TwoDifferentReleases_E2E(t *testing.T) {
	ctx := context.Background()
	client := newCommandTestNeo4jClient(t)
	pgDB := newCommandTestDB(t)

	wipeReleasePromotedFixtures(t, client)
	t.Cleanup(func() {
		wipeReleasePromotedFixtures(t, client)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM orchestrator_outbox WHERE event_type = 'release_promoted'`)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM message_processing WHERE stream_name = $1`, streams.ReleasePromotedV1)
	})

	repo := neo4jinfra.NewReleasePromotionRepository(client, newTestLogger())

	// ── First promotion: release rA with node "a" ─────────────────────────────

	cmdA := domainModel.PromoteReleaseInput{
		ReleaseID: "rA-twor-" + uuid.New().String()[:8],
		Topology: []domainEvent.ReleasePromotedNode{
			{
				UniqueID:          "svc.public.node_a",
				SchemaName:        "public",
				TableName:         "node_a",
				ServiceName:       "svc",
				ImageTag:          "tag-a",
				Schedule:          "daily",
				UpstreamUniqueIDs: []string{},
			},
		},
		ImageTags: map[string]string{"svc": "tag-a"},
	}

	handlerA := handlers.NewReleasePromotedHandler(
		pginfra.NewPostgresUnitOfWork(pgDB, newTestLogger()),
		repo,
		newTestLogger(),
	)
	require.NoError(t, handlerA.Handle(ctx, "msg-twor-A-"+uuid.New().String()[:8], cmdA))

	// ── Second promotion: release rB with node "c" ────────────────────────────

	relIDB := "rB-twor-" + uuid.New().String()[:8]
	cmdB := domainModel.PromoteReleaseInput{
		ReleaseID: relIDB,
		Topology: []domainEvent.ReleasePromotedNode{
			{
				UniqueID:          "svc.public.node_c",
				SchemaName:        "public",
				TableName:         "node_c",
				ServiceName:       "svc",
				ImageTag:          "tag-c",
				Schedule:          "hourly",
				UpstreamUniqueIDs: []string{},
			},
		},
		ImageTags: map[string]string{"svc": "tag-c"},
	}

	handlerB := handlers.NewReleasePromotedHandler(
		pginfra.NewPostgresUnitOfWork(pgDB, newTestLogger()),
		repo,
		newTestLogger(),
	)
	require.NoError(t, handlerB.Handle(ctx, "msg-twor-B-"+uuid.New().String()[:8], cmdB))

	// ── Neo4j: only node_c; node_a was truncated by rB ───────────────────────

	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)

	res, err := s.Run(ctx, `MATCH (t:Table) RETURN t.unique_id AS uid ORDER BY uid`, nil)
	require.NoError(t, err)
	var uids []string
	for res.Next(ctx) {
		uid, _ := res.Record().Get("uid")
		uids = append(uids, uid.(string))
	}
	assert.Equal(t, []string{"svc.public.node_c"}, uids,
		"rB promotion must truncate rA's nodes and create only rB's nodes")

	// :Meta reflects rB.
	res, err = s.Run(ctx, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS rid`, nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	rid, _ := res.Record().Get("rid")
	assert.Equal(t, relIDB, rid)

	// ── Outbox: 2 rows — one per promotion ───────────────────────────────────

	var outboxCount int
	require.NoError(t, pgDB.QueryRowContext(ctx,
		`SELECT count(*) FROM orchestrator_outbox WHERE event_type = 'release_promoted'`,
	).Scan(&outboxCount))
	assert.Equal(t, 2, outboxCount, "each distinct promotion must emit one schedules.loaded:v1 outbox row")
}
