//go:build integration

package handlers_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/export/graphschema"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// releasePromotedFixture is a handler wired to the test Neo4j through the
// unit of work, with the artifact read from memory, for a tenant of its own.
// The cleanup DETACH DELETEs every node of that tenant.
type releasePromotedFixture struct {
	t       *testing.T
	handler *handlers.ReleasePromotedHandler
	reader  *fakeArtifactReader
	client  neo4jinfra.Neo4jClient
	tenant  string
}

func newReleasePromotedFixture(t *testing.T) *releasePromotedFixture {
	t.Helper()
	ctx := context.Background()
	client, err := neo4jinfra.NewNeo4jClient(
		envOr("NEO4J_URI", "bolt://neo4j:7687"),
		envOr("NEO4J_USER", "neo4j"),
		envOr("NEO4J_PASSWORD", "atlas_password"),
		newTestLogger(),
	)
	if err != nil {
		testdeps.Unavailable(t, "Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(ctx) })
	require.NoError(t, neo4jinfra.InitSchema(ctx, client, newTestLogger()))

	f := &releasePromotedFixture{t: t, reader: &fakeArtifactReader{}, client: client, tenant: "handler-test-" + uuid.NewString()}
	f.handler = handlers.NewReleasePromotedHandler(neo4jinfra.NewUnitOfWork(client, newTestLogger()), f.reader, newTestLogger())
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer func() { _ = s.Close(ctx) }()
		for _, label := range graphschema.Labels() {
			stmt := fmt.Sprintf("MATCH (n:%s {tenant_id: $tenant_id}) DETACH DELETE n", label)
			res, err := s.Run(ctx, stmt, map[string]any{"tenant_id": f.tenant})
			if err == nil {
				_, err = res.Consume(ctx)
			}
			if err != nil {
				t.Errorf("cleanup %q: %v", stmt, err)
			}
		}
	})
	return f
}

// promotion builds promotion seq of the tenant's release releaseID carrying
// nodes, registering its artifact with the reader.
func (f *releasePromotedFixture) promotion(releaseID string, seq int64, nodes ...topologyartifact.Node) domainModel.PromoteReleaseInput {
	in := promotionOf(f.reader, topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion, TenantID: f.tenant, ReleaseID: releaseID, Nodes: nodes,
	}, seq)
	in.TenantID = f.tenant
	return in
}

// scalar runs a statement with $tenant_id bound to the fixture's tenant and
// returns column v of its first row, or nil when it returns none.
func (f *releasePromotedFixture) scalar(cypher string) any {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, cypher, map[string]any{"tenant_id": f.tenant})
	require.NoError(f.t, err)
	if !res.Next(ctx) {
		require.NoError(f.t, res.Err())
		return nil
	}
	v, _ := res.Record().Get("v")
	return v
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

func TestReleasePromotedConsumer_HappyPath_E2E(t *testing.T) {
	ctx := context.Background()
	f := newReleasePromotedFixture(t)
	in := f.promotion("rA", 1,
		modelNode("service-a.public.table_a", "table_a", "service-a", "tag-a", "daily"),
		modelNode("service-b.public.table_b", "table_b", "service-b", "tag-b", "daily", "service-a.public.table_a"),
	)

	require.NoError(t, f.handler.Handle(ctx, "msg-int-happy", nil, in))

	assert.Equal(t, int64(2), f.scalar(`MATCH (n:Node {tenant_id: $tenant_id}) RETURN count(n) AS v`))
	assert.Equal(t, "service-a.public.table_a", f.scalar(`
		MATCH (:Node {tenant_id: $tenant_id, unique_id: 'service-b.public.table_b'})-[:DEPENDS_ON]->(a:Node {tenant_id: $tenant_id})
		RETURN a.unique_id AS v`))
	assert.Equal(t, "rA", f.scalar(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN m.release_id AS v`))
	assert.Equal(t, int64(1), f.scalar(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN m.promotion_seq AS v`))
	assert.Equal(t, "daily", f.scalar(`
		MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'service-a.public.table_a'}) RETURN n.schedule_name AS v`))
	assert.Equal(t, streams.CatalogReleaseTopology, f.scalar(`
		MATCH (p:ProcessedMessage {tenant_id: $tenant_id, message_id: 'msg-int-happy'}) RETURN p.group AS v`),
		"the marker commits with the swap")
}

// The same promotion delivered again under the same message id is skipped by
// its marker; under a second message id it is a redelivery that writes only
// its own marker.
func TestReleasePromotedConsumer_Idempotent_E2E(t *testing.T) {
	ctx := context.Background()
	f := newReleasePromotedFixture(t)
	in := f.promotion("rA-idem", 1, modelNode("svc.public.t", "t", "svc", "tag-1", "hourly"))

	require.NoError(t, f.handler.Handle(ctx, "msg-idem-1", nil, in))
	f.reader.calls = nil
	require.NoError(t, f.handler.Handle(ctx, "msg-idem-1", nil, in))
	assert.Empty(t, f.reader.calls, "a processed message is acknowledged without reading the artifact")
	require.NoError(t, f.handler.Handle(ctx, "msg-idem-2", nil, in))

	assert.Equal(t, int64(1), f.scalar(`MATCH (n:Node {tenant_id: $tenant_id}) RETURN count(n) AS v`))
	assert.Equal(t, int64(2), f.scalar(`MATCH (p:ProcessedMessage {tenant_id: $tenant_id}) RETURN count(p) AS v`))
}

// A second release retires the node it drops and keeps it.
func TestReleasePromotedConsumer_TwoDifferentReleases_E2E(t *testing.T) {
	ctx := context.Background()
	f := newReleasePromotedFixture(t)

	require.NoError(t, f.handler.Handle(ctx, "msg-twor-A", nil,
		f.promotion("rA-twor", 1, modelNode("svc.public.node_a", "node_a", "svc", "tag-a", "daily"))))
	require.NoError(t, f.handler.Handle(ctx, "msg-twor-B", nil,
		f.promotion("rB-twor", 2, modelNode("svc.public.node_c", "node_c", "svc", "tag-c", "hourly"))))

	assert.Equal(t, "svc.public.node_c", f.scalar(`MATCH (n:Node {tenant_id: $tenant_id}) WHERE n.active RETURN n.unique_id AS v`))
	assert.Equal(t, false, f.scalar(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'svc.public.node_a'}) RETURN n.active AS v`),
		"the dropped node is retired and kept")
	assert.Equal(t, "rB-twor", f.scalar(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN m.release_id AS v`))
}

// A promotion older than the live one arrives after it: the topology stays on
// the newer release.
func TestReleasePromotedConsumer_LateOlderPromotion_E2E(t *testing.T) {
	ctx := context.Background()
	f := newReleasePromotedFixture(t)

	require.NoError(t, f.handler.Handle(ctx, "msg-late-new", nil, f.promotion("rNew", 2,
		seedNode("seeds.same", "img-new", "sha256:h1"),
		seedNode("seeds.changed_again", "img-new", "sha256:h2-new"),
	)))
	require.NoError(t, f.handler.Handle(ctx, "msg-late-old", nil, f.promotion("rOld", 1,
		seedNode("seeds.same", "img-old", "sha256:h1"),
		seedNode("seeds.changed_again", "img-old", "sha256:h2-old"),
		seedNode("seeds.removed", "img-old", "sha256:h3"),
	)))

	assert.Equal(t, "rNew", f.scalar(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN m.release_id AS v`))
	assert.Equal(t, int64(2), f.scalar(`MATCH (m:Meta {tenant_id: $tenant_id, key: 'current_release'}) RETURN m.promotion_seq AS v`))
	assert.Equal(t, "img-new", f.scalar(`
		MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'seeds.changed_again'}) RETURN n.image_tag AS v`))
	assert.Nil(t, f.scalar(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: 'seeds.removed'}) RETURN n.unique_id AS v`),
		"the stale promotion writes no node")
}
