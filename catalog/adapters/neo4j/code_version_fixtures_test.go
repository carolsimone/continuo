package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// The code-version query tests read the graph of versionFixtureTenant through
// CodeVersionQueryRepository. Their fixtures write versions with the versions
// group's repository and seed the nodes those versions hang off, every node
// under that tenant.

// versionFixtureTenant is the tenant the query tests' nodes and versions are
// written under.
const versionFixtureTenant = "version-query-test"

// wipeVersionFixtures deletes every node of versionFixtureTenant, whatever
// its label, so each test starts from a known-empty slate. No other tenant's
// node is touched.
func wipeVersionFixtures(t *testing.T, client neo4jinfra.Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (n {tenant_id: $tenant_id}) DETACH DELETE n`,
		map[string]any{"tenant_id": versionFixtureTenant})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedVersionNode creates the live node a version write attaches to, carrying
// the content_hash a topology swap would have stamped on it.
func seedVersionNode(t *testing.T, client neo4jinfra.Neo4jClient, uniqueID, contentHash string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx,
		`MERGE (n:Node {tenant_id: $tenant_id, unique_id: $uid}) SET n.content_hash = $ch, n.active = true`,
		map[string]any{"tenant_id": versionFixtureTenant, "uid": uniqueID, "ch": contentHash})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// versionWriter writes one release's code versions for versionFixtureTenant
// in one write of the unit of work, as one batch of the versions group.
type versionWriter struct {
	uow uow.UnitOfWork
}

func newVersionRepo(client neo4jinfra.Neo4jClient) *versionWriter {
	return &versionWriter{uow: neo4jinfra.NewUnitOfWork(client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))}
}

func (w *versionWriter) Write(ctx context.Context, in codeversion.WriteInput) (codeversion.WriteResult, error) {
	var res codeversion.WriteResult
	m := uow.Marker{TenantID: versionFixtureTenant, Group: streams.CatalogReleaseVersions, MessageID: uuid.NewString()}
	err := w.uow.Write(ctx, m, true, func(ctx context.Context, tx uow.Tx) error {
		var err error
		res, err = tx.CodeVersions().WriteBatch(ctx, versionFixtureTenant, in)
		return err
	})
	return res, err
}

func nodeInput(uniqueID, hash string, refs ...codeversion.UnitRef) codeversion.NodeVersion {
	return codeversion.NodeVersion{
		UniqueID: uniqueID, ContentHash: hash, SourceHash: "s", SharedCodeHash: "m",
		ConfigHash: "c", Runtime: "dbt", RawCode: "select 1", CompiledCode: "select 1",
		ConfigJSON: `{"materialized":"table"}`, UnitRefs: refs,
	}
}

// versionWriteInput is the promotion of releaseID at time at carrying nodes
// and units. Its promotion seq follows its time, so promotions a test orders
// by time are ordered by seq too; each node's artifact entry is a dbt model.
func versionWriteInput(releaseID string, at time.Time, nodes []codeversion.NodeVersion,
	units []codeversion.CodeUnitVersion) codeversion.WriteInput {
	topo := make(map[string]topology.ReleasePromotedTopologyNode, len(nodes))
	for _, n := range nodes {
		topo[n.UniqueID] = topology.ReleasePromotedTopologyNode{UniqueID: n.UniqueID, NodeType: "dbt-model"}
	}
	return codeversion.WriteInput{
		ReleaseID: releaseID, PromotionSeq: at.UnixMilli(), Repo: "org/svc", CommitSHA: "sha-" + releaseID,
		PromotedAt: at, Nodes: nodes, Units: units, Topology: topo,
	}
}
