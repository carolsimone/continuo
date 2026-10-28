package handlers_test

import (
	"context"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	pginfra "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wipeReleasePromotedFixtures removes the :Table nodes and the :Meta and
// :TopologyRoot singletons so each test starts clean.
func wipeReleasePromotedFixtures(t *testing.T, client neo4jinfra.Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	for _, q := range []string{
		`MATCH (n:Table) DETACH DELETE n`,
		`MATCH (m:Meta {key:'current_release'}) DELETE m`,
		`MATCH (root:TopologyRoot {id:'singleton'}) DELETE root`,
	} {
		_, err := s.Run(ctx, q, nil)
		require.NoError(t, err)
	}
}

// releasePromotedFixture wipes the graph and the rows these tests write, before
// and after the test, and returns a handler wired to real Neo4j and Postgres with
// the artifact read from memory.
func releasePromotedFixture(t *testing.T) (*handlers.ReleasePromotedHandler, *fakeArtifactReader, neo4jinfra.Neo4jClient, *sqlx.DB) {
	t.Helper()
	client := newCommandTestNeo4jClient(t)
	pgDB := newCommandTestDB(t)
	cleanup := func() {
		wipeReleasePromotedFixtures(t, client)
		_, _ = pgDB.ExecContext(context.Background(),
			`DELETE FROM message_processing WHERE stream_name = $1`, streams.OrchestratorReleasePromoted)
	}
	cleanup()
	t.Cleanup(cleanup)
	reader := &fakeArtifactReader{}
	h := handlers.NewReleasePromotedHandler(
		pginfra.NewPostgresUnitOfWork(pgDB, newTestLogger()),
		reader,
		neo4jinfra.NewReleasePromotionRepository(client, newTestLogger()),
		newTestLogger(),
	)
	return h, reader, client, pgDB
}

func artifactOf(releaseID string, nodes ...topologyartifact.Node) topologyartifact.Document {
	return topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion, TenantID: "default", ReleaseID: releaseID, Nodes: nodes,
	}
}

func modelNode(uid, table, service, image, schedule string, upstreams ...string) topologyartifact.Node {
	if upstreams == nil {
		upstreams = []string{}
	}
	return topologyartifact.Node{
		UniqueID: uid, SchemaName: "public", TableName: table, ServiceName: service,
		NodeType: "dbt-model", ContentHash: "sha256:" + uid, ImageTag: image, Schedule: schedule,
		UpstreamUniqueIDs: upstreams,
	}
}

func seedNode(uid, image, hash string) topologyartifact.Node {
	return topologyartifact.Node{
		UniqueID: uid, SchemaName: "seeds", TableName: uid, ServiceName: "svc",
		NodeType: "dbt-seed", ContentHash: hash, ImageTag: image, Schedule: "daily",
		UpstreamUniqueIDs: []string{},
	}
}

func graphScalar(t *testing.T, client neo4jinfra.Neo4jClient, cypher string) any {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, cypher, nil)
	require.NoError(t, err)
	if !res.Next(ctx) {
		require.NoError(t, res.Err())
		return nil
	}
	v, _ := res.Record().Get("v")
	return v
}

func TestReleasePromotedConsumer_HappyPath_E2E(t *testing.T) {
	ctx := context.Background()
	h, reader, client, pgDB := releasePromotedFixture(t)
	releaseID := "rA-" + uuid.New().String()[:8]
	in := promotionOf(reader, artifactOf(releaseID,
		modelNode("service-a.public.table_a", "table_a", "service-a", "tag-a", "daily"),
		modelNode("service-b.public.table_b", "table_b", "service-b", "tag-b", "daily", "service-a.public.table_a"),
	), 1)
	msgID := "msg-int-happy-" + uuid.New().String()[:8]

	require.NoError(t, h.Handle(ctx, msgID, nil, in))

	assert.Equal(t, int64(2), graphScalar(t, client, `MATCH (t:Table) RETURN count(t) AS v`))
	assert.Equal(t, "service-a.public.table_a", graphScalar(t, client,
		`MATCH (:Table {unique_id:'service-b.public.table_b'})-[:DEPENDS_ON]->(a:Table) RETURN a.unique_id AS v`))
	assert.Equal(t, releaseID, graphScalar(t, client, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS v`))
	assert.Equal(t, int64(1), graphScalar(t, client, `MATCH (m:Meta {key:'current_release'}) RETURN m.promotion_seq AS v`))
	assert.Equal(t, int64(1), graphScalar(t, client, `MATCH (r:TopologyRoot {id:'singleton'}) RETURN r.promotion_seq AS v`))

	assert.Equal(t, "daily", graphScalar(t, client,
		`MATCH (t:Table {unique_id:'service-a.public.table_a'}) RETURN t.schedule_name AS v`))

	var dedupCount int
	require.NoError(t, pgDB.QueryRowContext(ctx,
		`SELECT count(*) FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		msgID, streams.OrchestratorReleasePromoted).Scan(&dedupCount))
	assert.Equal(t, 1, dedupCount)
}

// The same promotion delivered under two message ids swaps once.
func TestReleasePromotedConsumer_Idempotent_E2E(t *testing.T) {
	ctx := context.Background()
	h, reader, client, _ := releasePromotedFixture(t)
	releaseID := "rA-idem-" + uuid.New().String()[:8]
	in := promotionOf(reader, artifactOf(releaseID, modelNode("svc.public.t", "t", "svc", "tag-1", "hourly")), 1)

	require.NoError(t, h.Handle(ctx, "msg-idem-1-"+uuid.New().String()[:8], nil, in))
	require.NoError(t, h.Handle(ctx, "msg-idem-2-"+uuid.New().String()[:8], nil, in))

	assert.Equal(t, int64(1), graphScalar(t, client, `MATCH (t:Table) RETURN count(t) AS v`))
}

func TestReleasePromotedConsumer_TwoDifferentReleases_E2E(t *testing.T) {
	ctx := context.Background()
	h, reader, client, _ := releasePromotedFixture(t)
	relA := "rA-twor-" + uuid.New().String()[:8]
	relB := "rB-twor-" + uuid.New().String()[:8]

	require.NoError(t, h.Handle(ctx, "msg-twor-A-"+uuid.New().String()[:8], nil,
		promotionOf(reader, artifactOf(relA, modelNode("svc.public.node_a", "node_a", "svc", "tag-a", "daily")), 1)))
	require.NoError(t, h.Handle(ctx, "msg-twor-B-"+uuid.New().String()[:8], nil,
		promotionOf(reader, artifactOf(relB, modelNode("svc.public.node_c", "node_c", "svc", "tag-c", "hourly")), 2)))

	assert.Equal(t, "svc.public.node_c", graphScalar(t, client, `MATCH (t:Table) RETURN t.unique_id AS v`))
	assert.Equal(t, relB, graphScalar(t, client, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS v`))
}

// A promotion older than the live one arrives after it: the topology stays on
// the newer release.
func TestReleasePromotedConsumer_LateOlderPromotion_E2E(t *testing.T) {
	ctx := context.Background()
	h, reader, client, _ := releasePromotedFixture(t)
	relNew := "rNew-" + uuid.New().String()[:8]
	relOld := "rOld-" + uuid.New().String()[:8]

	require.NoError(t, h.Handle(ctx, "msg-late-new-"+uuid.New().String()[:8], nil,
		promotionOf(reader, artifactOf(relNew,
			seedNode("seeds.same", "img-new", "sha256:h1"),
			seedNode("seeds.changed_again", "img-new", "sha256:h2-new"),
		), 2)))

	require.NoError(t, h.Handle(ctx, "msg-late-old-"+uuid.New().String()[:8], nil,
		promotionOf(reader, artifactOf(relOld,
			seedNode("seeds.same", "img-old", "sha256:h1"),
			seedNode("seeds.changed_again", "img-old", "sha256:h2-old"),
			seedNode("seeds.removed", "img-old", "sha256:h3"),
		), 1, "seeds.changed_again", "seeds.removed", "seeds.same")))

	assert.Equal(t, relNew, graphScalar(t, client, `MATCH (m:Meta {key:'current_release'}) RETURN m.release_id AS v`))
	assert.Equal(t, int64(2), graphScalar(t, client, `MATCH (m:Meta {key:'current_release'}) RETURN m.promotion_seq AS v`))
}
