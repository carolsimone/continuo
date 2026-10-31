//go:build integration

package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versionsGroupFixture drives both release.promoted groups of one tenant on
// the test Neo4j: the topology handler of releasePromotedFixture and the
// versions handler, each on the Neo4j unit of work, with every artifact and
// bundle read from memory.
type versionsGroupFixture struct {
	*releasePromotedFixture
	uow      uow.UnitOfWork
	bundles  *fakeBundleReader
	versions *handlers.ReleasePromotedVersionsHandler
}

func newVersionsGroupFixture(t *testing.T) *versionsGroupFixture {
	t.Helper()
	f := newReleasePromotedFixture(t)
	g := &versionsGroupFixture{
		releasePromotedFixture: f,
		uow:                    neo4jinfra.NewUnitOfWork(f.client, newTestLogger()),
		bundles:                &fakeBundleReader{bundles: map[string]codebundle.Bundle{}},
	}
	g.versions = handlers.NewReleasePromotedVersionsHandler(g.uow, g.bundles, f.reader, newTestLogger())
	return g
}

// versionedModel is a dbt model of the analytics schema whose code hashes to
// hash.
func versionedModel(uniqueID, hash string, upstreams ...string) topologyartifact.Node {
	if upstreams == nil {
		upstreams = []string{}
	}
	return topologyartifact.Node{
		UniqueID: uniqueID, SchemaName: "analytics", TableName: uniqueID, ServiceName: "svc",
		NodeType: "dbt-model", ContentHash: hash, ImageTag: "img", Schedule: "daily", UpstreamUniqueIDs: upstreams,
	}
}

// release builds promotion seq of releaseID for the fixture's tenant. Its
// artifact carries nodes; its bundle carries every node but the dbt-tests,
// as topology-controller writes it, each using the shared unit
// svc:macro.shared of this release.
func (g *versionsGroupFixture) release(releaseID string, seq int64, nodes ...topologyartifact.Node) domainModel.PromoteReleaseInput {
	in := g.promotion(releaseID, seq, nodes...)
	b := codebundle.Bundle{ContractVersion: 1, ReleaseID: releaseID, Nodes: map[string]codebundle.Node{},
		SharedCode: map[string]codebundle.SharedCodeUnit{
			"svc:macro.shared": {Source: "macro of " + releaseID, Checksum: "shared-" + releaseID},
		}}
	for _, n := range nodes {
		if n.NodeType == "dbt-test" {
			continue
		}
		b.Nodes[n.UniqueID] = codebundle.Node{Runtime: "dbt", RawCode: "select '" + n.ContentHash + "'",
			ContentHash: n.ContentHash, CodeUnitIDs: []string{"svc:macro.shared"}}
		in.ChangedNodeIDs = append(in.ChangedNodeIDs, n.UniqueID)
	}
	in.CodeBundleURI = "s3://continuo/code-bundles/" + g.tenant + "/" + releaseID + "/bundle.json"
	g.bundles.bundles[in.CodeBundleURI] = b
	in.Repo = "org/svc"
	in.CommitSHA = "sha-" + releaseID
	in.PromotedAt = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Hour)
	return in
}

// rows runs a read statement with $tenant_id bound to the fixture's tenant.
func (g *versionsGroupFixture) rows(cypher string, params map[string]any) []*neo4j.Record {
	g.t.Helper()
	ctx := context.Background()
	s := g.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	bound := map[string]any{"tenant_id": g.tenant}
	for k, v := range params {
		bound[k] = v
	}
	res, err := s.Run(ctx, cypher, bound)
	require.NoError(g.t, err)
	recs, err := res.Collect(ctx)
	require.NoError(g.t, err)
	return recs
}

// node returns the properties of the tenant's :Node uniqueID, or nil.
func (g *versionsGroupFixture) node(uniqueID string) map[string]any {
	g.t.Helper()
	recs := g.rows(`MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid}) RETURN properties(n) AS p`, map[string]any{"uid": uniqueID})
	require.LessOrEqual(g.t, len(recs), 1)
	if len(recs) == 0 {
		return nil
	}
	p, _ := recs[0].Get("p")
	return p.(map[string]any)
}

// current returns the content_hash of uniqueID's :CURRENT version, or "".
func (g *versionsGroupFixture) current(uniqueID string) string {
	g.t.Helper()
	recs := g.rows(`MATCH (:Node {tenant_id: $tenant_id, unique_id: $uid})-[:CURRENT]->(v:NodeVersion {tenant_id: $tenant_id})
		RETURN v.content_hash AS v`, map[string]any{"uid": uniqueID})
	require.LessOrEqual(g.t, len(recs), 1)
	if len(recs) == 0 {
		return ""
	}
	v, _ := recs[0].Get("v")
	return v.(string)
}

// processed reports whether the versions group's marker of messageID is
// recorded for the tenant.
func (g *versionsGroupFixture) processed(messageID string) bool {
	g.t.Helper()
	ok, err := g.uow.Processed(context.Background(),
		uow.Marker{TenantID: g.tenant, Group: streams.CatalogReleaseVersions, MessageID: messageID})
	require.NoError(g.t, err)
	return ok
}

// graph renders every node of the tenant and every relationship from one as
// sorted lines, without tenant_id and leaving out the nodes of the labels in
// skip, so the graphs of two tenants compare equal when they hold the same
// data.
func (g *versionsGroupFixture) graph(skip ...string) []string {
	g.t.Helper()
	if skip == nil {
		skip = []string{}
	}
	strip := func(v any) map[string]any {
		p := v.(map[string]any)
		delete(p, "tenant_id")
		return p
	}
	var lines []string
	for _, rec := range g.rows(`
		MATCH (n {tenant_id: $tenant_id}) WHERE none(l IN labels(n) WHERE l IN $skip)
		RETURN labels(n) AS l, properties(n) AS p`, map[string]any{"skip": skip}) {
		l, _ := rec.Get("l")
		p, _ := rec.Get("p")
		lines = append(lines, fmt.Sprint("node ", l, strip(p)))
	}
	for _, rec := range g.rows(`
		MATCH (a {tenant_id: $tenant_id})-[r]->(b) WHERE none(l IN labels(a) + labels(b) WHERE l IN $skip)
		RETURN properties(a) AS a, type(r) AS t, properties(r) AS p, properties(b) AS b`, map[string]any{"skip": skip}) {
		a, _ := rec.Get("a")
		typ, _ := rec.Get("t")
		p, _ := rec.Get("p")
		b, _ := rec.Get("b")
		lines = append(lines, fmt.Sprint("rel ", strip(a), " -", typ, p, "-> ", strip(b)))
	}
	slices.Sort(lines)
	return lines
}

// TestVersionsGroup_OutOfOrder is design §6.3's table: r8 (seq 8) adds
// table_e and drops table_d, and its versions arrive before its swap.
//
//  1. The versions group handles r8 first: table_e is absent, so it is
//     created inactive with its version and :CURRENT (watermark 8).
//  2. The topology group handles r8: table_e is live, with its version.
//  3. r7 is redriven to both groups: the swap is stale and writes only its
//     marker; the versions group finds r7's versions, and the watermark 8 > 7
//     keeps every :CURRENT. The graph is unchanged.
func TestVersionsGroup_OutOfOrder(t *testing.T) {
	ctx := context.Background()
	g := newVersionsGroupFixture(t)
	const (
		a = "analytics.table_a"
		d = "analytics.table_d"
		e = "analytics.table_e"
	)
	r7 := g.release("r7", 7, versionedModel(a, "sha256:a7"), versionedModel(d, "sha256:d7", a))
	require.NoError(t, g.handler.Handle(ctx, "r7-topology", nil, r7))
	require.NoError(t, g.versions.Handle(ctx, "r7-versions", nil, r7))
	require.Equal(t, "sha256:a7", g.current(a))
	r8 := g.release("r8", 8, versionedModel(a, "sha256:a8"), versionedModel(e, "sha256:e8", a),
		topologyartifact.Node{UniqueID: "test.table_e_not_null", ServiceName: "svc", NodeType: "dbt-test", UpstreamUniqueIDs: []string{e}})

	// 1. The versions of r8 land before its swap.
	require.NoError(t, g.versions.Handle(ctx, "r8-versions", nil, r8))
	inactive := g.node(e)
	require.NotNil(t, inactive, "the versions group creates the node the swap has not written yet")
	assert.Equal(t, false, inactive["active"])
	assert.NotContains(t, inactive, "retired_at")
	assert.Equal(t, "r8", inactive["release_id"])
	assert.Equal(t, "analytics", inactive["schema_name"])
	assert.Equal(t, int64(8), inactive["code_version_promotion_seq"])
	assert.Equal(t, "sha256:e8", g.current(e), "the inactive node has its current version")
	assert.Equal(t, "sha256:a8", g.current(a))
	assert.Equal(t, true, g.node(d)["active"], "the swap of r8 has not run")
	assert.Nil(t, g.node("test.table_e_not_null"), "a dbt-test is never a node")

	// 2. The swap of r8 makes table_e live; it keeps its version.
	require.NoError(t, g.handler.Handle(ctx, "r8-topology", nil, r8))
	assert.Equal(t, true, g.node(e)["active"])
	assert.Equal(t, "sha256:e8", g.current(e))
	assert.Equal(t, false, g.node(d)["active"])
	assert.Equal(t, "sha256:d7", g.current(d), "the retired node keeps its last version")

	// 3. r7 redriven to both groups changes nothing.
	before := g.graph("ProcessedMessage")
	require.NoError(t, g.handler.Handle(ctx, "r7-topology-redriven", nil, r7))
	require.NoError(t, g.versions.Handle(ctx, "r7-versions-redriven", nil, r7))
	assert.Equal(t, before, g.graph("ProcessedMessage"), "a redriven older promotion leaves the graph unchanged")
	assert.Equal(t, "sha256:a8", g.current(a))
	assert.True(t, g.processed("r7-versions-redriven"))
}

// errCrash stands in for a process that dies before its transaction commits.
var errCrash = errors.New("crashed before the commit")

// crashingUnitOfWork is a unit of work whose Write number crashAt runs its
// batch's statements and then fails, so that batch's transaction rolls back.
type crashingUnitOfWork struct {
	uow.UnitOfWork
	crashAt int
	writes  int
}

func (c *crashingUnitOfWork) Write(ctx context.Context, m uow.Marker, final bool, fn func(ctx context.Context, tx uow.Tx) error) error {
	c.writes++
	if c.writes != c.crashAt {
		return c.UnitOfWork.Write(ctx, m, final, fn)
	}
	return c.UnitOfWork.Write(ctx, m, final, func(ctx context.Context, tx uow.Tx) error {
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return errCrash
	})
}

// A crash during batch k+1 of n leaves batches 1..k committed and no marker.
// The redelivery writes every batch again; each MERGE finds what batches
// 1..k wrote, and the graph is the one a delivery without a crash leaves.
func TestVersionsHandler_CrashAfterBatchKReplaysEveryBatch(t *testing.T) {
	ctx := context.Background()
	const k = 2
	nodes := make([]topologyartifact.Node, 0, 3*handlers.VersionsBatchSize+50)
	for i := 0; i < cap(nodes); i++ {
		nodes = append(nodes, versionedModel(fmt.Sprintf("analytics.n%03d", i), fmt.Sprintf("sha256:%03d", i)))
	}
	crashed := newVersionsGroupFixture(t)
	clean := newVersionsGroupFixture(t)
	in := crashed.release("r1", 1, nodes...)

	crash := &crashingUnitOfWork{UnitOfWork: crashed.uow, crashAt: k + 1}
	err := handlers.NewReleasePromotedVersionsHandler(crash, crashed.bundles, crashed.reader, newTestLogger()).
		Handle(ctx, "1-0", nil, in)
	require.ErrorIs(t, err, errCrash)
	assert.Equal(t, k+1, crash.writes, "no batch after the crash is written")
	assert.False(t, crashed.processed("1-0"), "the marker commits with the last batch only")
	written := crashed.rows(`MATCH (v:NodeVersion {tenant_id: $tenant_id}) RETURN count(v) AS n`, nil)
	n, _ := written[0].Get("n")
	assert.Equal(t, int64(k*handlers.VersionsBatchSize), n, "batches 1..k committed and batch k+1 rolled back")

	require.NoError(t, crashed.versions.Handle(ctx, "1-0", nil, in))
	assert.True(t, crashed.processed("1-0"))

	require.NoError(t, clean.versions.Handle(ctx, "1-0", nil, clean.release("r1", 1, nodes...)))
	assert.Equal(t, clean.graph("ProcessedMessage"), crashed.graph("ProcessedMessage"),
		"the replay leaves the graph a delivery without a crash leaves")
}
