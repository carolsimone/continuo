//go:build integration

package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain"
	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tenants A and B hold the same nodes under the same unique_ids, written by the
// same messages. Every value a read can return carries its tenant's mark
// (A's isoMarkA, B's isoMarkB) or its tenant's test count, and B alone holds
// isoOnlyB, isoOnlyBUnit and isoOnlyBSig. Every repository method then runs
// as A, and B's graph must be unchanged after each call, and no read may
// return what B holds.

const (
	isoMarkA = "amark"
	isoMarkB = "bmark"

	isoUp        = "iso.table_up"
	isoDown      = "iso.table_down"
	isoOnlyB     = "iso.only_b"
	isoUnit      = "svc:iso_macro"
	isoOnlyBUnit = "svc:iso_only_b_macro"
	isoSig       = "sig-iso"
	isoOnlyBSig  = "sig-iso-only-b"
	isoProposal  = "prop-iso"
	isoHolder    = "iso-holder"
)

// isolationRow is one repository method, run as tenant A, and the checks of
// what it returned.
type isolationRow struct {
	method string
	run    func(t *testing.T)
}

// isolationRepositories are the repositories of this package: the readers
// the gRPC server serves from, the three that write through uow.Tx, the
// export lease and the unit of work. Each exported method of each has a row.
var isolationRepositories = []any{
	(*NodeQueryRepository)(nil),
	(*CodeVersionQueryRepository)(nil),
	(*PrecedentQueryRepository)(nil),
	(*releasePromotionRepository)(nil),
	(*codeVersionRepository)(nil),
	(*caseBaseRepository)(nil),
	(*exportLease)(nil),
	(*unitOfWork)(nil),
}

type isolationFixture struct {
	t          *testing.T
	a, b       *swapFixture
	ctx        context.Context
	nodes      *NodeQueryRepository
	versions   *CodeVersionQueryRepository
	precedents *PrecedentQueryRepository
	lease      ports.ExportLease
	// sharedMessage is a message id B's topology group processed; A never did.
	sharedMessage string
	// wroteA reports whether the unit of work ran A's write.
	wroteA bool
}

func TestRepositories_TenantIsolation(t *testing.T) {
	f := newIsolationFixture(t)
	before := f.snapshotB()
	for _, row := range f.rows() {
		t.Run(row.method, func(t *testing.T) {
			row.run(t)
			assert.Equal(t, before, f.snapshotB(), "%s as tenant A changed tenant B's graph", row.method)
		})
	}
}

// TestRepositories_TenantIsolationCoversEveryMethod fails when a repository
// of this package has an exported method TestRepositories_TenantIsolation
// does not run, or a row names a method no repository has.
func TestRepositories_TenantIsolationCoversEveryMethod(t *testing.T) {
	rows := map[string]bool{}
	for _, row := range (&isolationFixture{}).rows() {
		require.False(t, rows[row.method], "two rows for %s", row.method)
		rows[row.method] = true
	}
	methods := map[string]bool{}
	for _, repo := range isolationRepositories {
		typ := reflect.TypeOf(repo)
		for i := range typ.NumMethod() {
			name := typ.Elem().Name() + "." + typ.Method(i).Name
			methods[name] = true
			assert.True(t, rows[name], "%s has no row in TestRepositories_TenantIsolation", name)
		}
	}
	for name := range rows {
		assert.True(t, methods[name], "the row %s names no method of a repository", name)
	}
}

// TestRepositories_EveryTenantScopedTypeIsListed fails when a type of this
// package declares an exported method that takes a tenant and is not one of
// isolationRepositories, so a new repository cannot escape the isolation test.
func TestRepositories_EveryTenantScopedTypeIsListed(t *testing.T) {
	listed := map[string]bool{}
	for _, repo := range isolationRepositories {
		listed[reflect.TypeOf(repo).Elem().Name()] = true
	}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, e.Name(), nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() || !takesTenant(fn) {
				continue
			}
			recv := fn.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			name := recv.(*ast.Ident).Name
			assert.True(t, listed[name], "%s.%s takes a tenant, but %s is not in isolationRepositories", name, fn.Name.Name, name)
		}
	}
}

func takesTenant(fn *ast.FuncDecl) bool {
	for _, field := range fn.Type.Params.List {
		for _, name := range field.Names {
			if name.Name == "tenantID" {
				return true
			}
		}
	}
	return false
}

func newIsolationFixture(t *testing.T) *isolationFixture {
	t.Helper()
	a := newSwapFixture(t)
	f := &isolationFixture{
		t: t, a: a, b: a.another(), ctx: context.Background(),
		nodes:         NewNodeQueryRepository(a.client, quietLogger()),
		versions:      NewCodeVersionQueryRepository(a.client, quietLogger()),
		precedents:    NewPrecedentQueryRepository(a.client, quietLogger()),
		lease:         NewExportLease(a.client, quietLogger()),
		sharedMessage: "iso-shared-" + uuid.NewString(),
	}
	seedIsolationTenant(f.a, isoMarkA, 1, false)
	seedIsolationTenant(f.b, isoMarkB, 2, true)

	// B has processed sharedMessage and holds its export lease, so neither may
	// skip or pause a write of A.
	require.NoError(t, f.b.uow.Write(f.ctx, f.topologyMarker(f.b.tenant), true, func(context.Context, uow.Tx) error { return nil }))
	require.NoError(t, f.lease.Acquire(f.ctx, f.b.tenant, ports.LeaseExport, "b-holder", 10*time.Minute))
	return f
}

// seedIsolationTenant writes one tenant's graph: releases r1 and r2 carrying
// table_up and table_down (down depends on up), two versions of each, a shared
// unit with two versions, and a failure of table_down that a version and a
// merged fix PR, which edited table_up, resolved. Each value a read returns
// carries mark; withOnlyB adds the node, unit and signature only B holds.
func seedIsolationTenant(f *swapFixture, mark string, testCount int, withOnlyB bool) {
	f.t.Helper()
	node := func(uniqueID string, upstreams ...string) topology.ReleasePromotedTopologyNode {
		n := modelNode(uniqueID, upstreams...)
		n.TestCount = testCount
		n.OriginalFilePath = "models/" + uniqueID + "-" + mark + ".sql"
		return n
	}
	nodes := releaseOf(node(isoUp), node(isoDown, isoUp))
	if withOnlyB {
		nodes = append(nodes, node(isoOnlyB))
	}
	for _, seq := range []int64{1, 2} {
		rel := fmt.Sprintf("r%d", seq)
		f.promoteAt(rel, seq, nodes, promotedAtOf(seq))
		unitSum := fmt.Sprintf("u%d-%s", seq, mark)
		in := batchOf(rel, seq,
			versionOf(isoUp, fmt.Sprintf("up%d-%s", seq, mark), codeversion.UnitRef{UnitID: isoUnit, Checksum: unitSum}),
			versionOf(isoDown, fmt.Sprintf("down%d-%s", seq, mark)))
		units := []codeversion.CodeUnitVersion{{UnitID: isoUnit, Checksum: unitSum, Source: "source-" + mark}}
		if withOnlyB {
			in.Nodes = append(in.Nodes, versionOf(isoOnlyB, fmt.Sprintf("onlyb%d-%s", seq, mark),
				codeversion.UnitRef{UnitID: isoOnlyBUnit, Checksum: unitSum}))
			in.Topology[isoOnlyB] = modelNode(isoOnlyB)
			units = append(units, codeversion.CodeUnitVersion{UnitID: isoOnlyBUnit, Checksum: unitSum, Source: "source-" + mark})
		}
		f.writeBatch(withUnits(in, units...))
	}

	rej := rejectionOf("r-failed", isoDown, isoSig, promotedAtOf(1).Add(30*time.Minute))
	rej.ErrorExcerpt = "excerpt-" + mark
	f.recordRejection(rej)
	if withOnlyB {
		f.recordRejection(rejectionOf("r-failed", isoOnlyB, isoOnlyBSig, promotedAtOf(1).Add(30*time.Minute)))
	}
	pr := openPullRequest(isoProposal, "svc", 7, promotedAtOf(2))
	pr.PrURL = "https://github.com/org/repo/pull/" + mark
	f.recordProposal(casebase.Proposal{ProposalID: isoProposal, ReleaseID: "r-failed", NodeID: isoDown}, pr)
	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: isoProposal, ReleaseID: "r-failed", Service: "svc", Outcome: "merged",
		ClosedAt: promotedAtOf(2), ResolvedNodeIDs: []string{isoDown},
		Edits: []casebase.EditOutcome{{Path: "models/up-" + mark + ".sql", TargetNodeID: isoUp, Diff: "diff-" + mark}},
	})
}

func (f *isolationFixture) topologyMarker(tenant string) uow.Marker {
	return uow.Marker{TenantID: tenant, Group: streams.CatalogReleaseTopology, MessageID: f.sharedMessage}
}

// snapshotB renders every node of tenant B, its processed-message markers and
// its export lease included, and every relationship that touches one, from
// either side, as sorted lines.
func (f *isolationFixture) snapshotB() []string {
	f.t.Helper()
	var lines []string
	for _, rec := range f.b.rows(`MATCH (n {tenant_id: $tenant_id}) RETURN labels(n) AS l, properties(n) AS p`, nil) {
		l, _ := rec.Get("l")
		p, _ := rec.Get("p")
		lines = append(lines, fmt.Sprint("node ", l, p))
	}
	for _, rec := range f.b.rows(`
		MATCH (a)-[r]->(b) WHERE a.tenant_id = $tenant_id OR b.tenant_id = $tenant_id
		RETURN properties(a) AS a, type(r) AS t, properties(r) AS p, properties(b) AS b`, nil) {
		a, _ := rec.Get("a")
		typ, _ := rec.Get("t")
		p, _ := rec.Get("p")
		b, _ := rec.Get("b")
		lines = append(lines, fmt.Sprint("rel ", a, " -", typ, p, "-> ", b))
	}
	slices.Sort(lines)
	return lines
}

// onlyA checks a read's result holds what A wrote and nothing B wrote.
func onlyA(t *testing.T, result any) {
	t.Helper()
	text := fmt.Sprintf("%+v", result)
	assert.Contains(t, text, isoMarkA, "the read returned none of tenant A's data")
	assert.NotContains(t, text, isoMarkB, "the read returned tenant B's data")
}

// rows is one row per exported method of isolationRepositories, in the order
// TestRepositories_TenantIsolation runs them: the reads first, on the seeded
// graph, then the writes.
func (f *isolationFixture) rows() []isolationRow {
	ctx := context.Background()
	tenant := func() string { return f.a.tenant }
	return []isolationRow{
		{"NodeQueryRepository.GetNode", func(t *testing.T) {
			meta, err := f.nodes.GetNode(ctx, tenant(), "svc", "analytics", isoUp, domain.ActiveNodesOnly)
			require.NoError(t, err)
			assert.Equal(t, 1, meta.TestCount, "tenant A's node, not B's")
			_, err = f.nodes.GetNode(ctx, tenant(), "svc", "analytics", isoOnlyB, domain.IncludeInactiveNodes)
			assert.ErrorIs(t, err, domain.ErrNodeNotFound)
		}},
		{"NodeQueryRepository.GetNodeLocation", func(t *testing.T) {
			loc, err := f.nodes.GetNodeLocation(ctx, tenant(), isoUp)
			require.NoError(t, err)
			onlyA(t, loc)
			_, err = f.nodes.GetNodeLocation(ctx, tenant(), isoOnlyB)
			assert.ErrorIs(t, err, domain.ErrNodeNotFound)
		}},
		{"CodeVersionQueryRepository.NodeVersions", func(t *testing.T) {
			vs, err := f.versions.NodeVersions(ctx, tenant(), isoUp, 10, true)
			require.NoError(t, err)
			require.Len(t, vs, 2)
			onlyA(t, vs)
			_, err = f.versions.NodeVersions(ctx, tenant(), isoOnlyB, 10, true)
			assert.ErrorIs(t, err, domain.ErrNodeNotFound)
		}},
		{"CodeVersionQueryRepository.CurrentNodeVersion", func(t *testing.T) {
			vs, err := f.versions.CurrentNodeVersion(ctx, tenant(), isoUp, true)
			require.NoError(t, err)
			require.Len(t, vs, 1)
			onlyA(t, vs)
			_, err = f.versions.CurrentNodeVersion(ctx, tenant(), isoOnlyB, true)
			assert.ErrorIs(t, err, domain.ErrNodeNotFound)
		}},
		{"CodeVersionQueryRepository.VersionsBySeq", func(t *testing.T) {
			from, to, err := f.versions.VersionsBySeq(ctx, tenant(), isoUp, 1, 2)
			require.NoError(t, err)
			onlyA(t, []codeversion.VersionView{from, to})
			assert.True(t, to.IsCurrent)
			_, _, err = f.versions.VersionsBySeq(ctx, tenant(), isoOnlyB, 1, 2)
			assert.ErrorIs(t, err, domain.ErrNodeNotFound)
		}},
		{"CodeVersionQueryRepository.Ancestors", func(t *testing.T) {
			ancestors, err := f.versions.Ancestors(ctx, tenant(), isoDown, 2, time.Time{}, 5)
			require.NoError(t, err)
			require.Len(t, ancestors, 1)
			assert.Equal(t, isoUp, ancestors[0].UniqueID)
			require.Len(t, ancestors[0].Versions, 2)
			onlyA(t, ancestors)
			ancestors, err = f.versions.Ancestors(ctx, tenant(), isoOnlyB, 2, time.Time{}, 5)
			require.NoError(t, err)
			assert.Empty(t, ancestors)
		}},
		{"CodeVersionQueryRepository.UnitVersions", func(t *testing.T) {
			vs, err := f.versions.UnitVersions(ctx, tenant(), isoUnit, 10)
			require.NoError(t, err)
			require.Len(t, vs, 2)
			onlyA(t, vs)
			_, err = f.versions.UnitVersions(ctx, tenant(), isoOnlyBUnit, 10)
			assert.ErrorIs(t, err, domain.ErrUnitNotFound)
		}},
		{"CodeVersionQueryRepository.UnitVersionsBatch", func(t *testing.T) {
			byUnit, err := f.versions.UnitVersionsBatch(ctx, tenant(), []string{isoUnit, isoOnlyBUnit}, 10)
			require.NoError(t, err)
			require.Len(t, byUnit[isoUnit], 2)
			assert.NotContains(t, byUnit, isoOnlyBUnit)
			onlyA(t, byUnit)
		}},
		{"CodeVersionQueryRepository.UnitsForNode", func(t *testing.T) {
			units, err := f.versions.UnitsForNode(ctx, tenant(), isoUp)
			require.NoError(t, err)
			assert.Equal(t, []string{isoUnit}, units)
			units, err = f.versions.UnitsForNode(ctx, tenant(), isoOnlyB)
			require.NoError(t, err)
			assert.Empty(t, units)
		}},
		{"PrecedentQueryRepository.Precedents", func(t *testing.T) {
			bySig, err := f.precedents.Precedents(ctx, tenant(), isoSig, "", "", 10, true)
			require.NoError(t, err)
			require.Len(t, bySig, 1)
			require.NotNil(t, bySig[0].ResolvingVersion)
			require.Len(t, bySig[0].Proposals, 1)
			require.Len(t, bySig[0].Edited, 1)
			onlyA(t, bySig)
			byReason, err := f.precedents.Precedents(ctx, tenant(), "", "cat", "reason", 10, true)
			require.NoError(t, err)
			require.Len(t, byReason, 1, "the category and reason match A's one rejection, not B's two")
			onlyA(t, byReason)
			none, err := f.precedents.Precedents(ctx, tenant(), isoOnlyBSig, "", "", 10, true)
			require.NoError(t, err)
			assert.Empty(t, none)
		}},
		{"releasePromotionRepository.PromoteRelease", func(t *testing.T) {
			assert.Equal(t, topology.PromotionApplied, f.a.promoteAt("r3", 3, releaseOf(modelNode(isoUp)), promotedAtOf(3)))
			assert.Equal(t, []string{isoUp}, f.a.activeIDs())
		}},
		{"codeVersionRepository.WriteBatch", func(t *testing.T) {
			f.a.writeBatch(batchOf("r3", 3, versionOf(isoUp, "up3-"+isoMarkA), versionOf(isoOnlyB, "onlyb3-"+isoMarkA)))
			assert.Equal(t, "up3-"+isoMarkA, f.a.current(isoUp))
			assert.Equal(t, "onlyb3-"+isoMarkA, f.a.current(isoOnlyB), "A gets its own inactive node, not B's")
		}},
		{"caseBaseRepository.RecordRejection", func(t *testing.T) {
			f.a.recordRejection(rejectionOf("r-failed-again", isoOnlyB, isoOnlyBSig, promotedAtOf(3)))
			assert.Equal(t, int64(1), f.a.failedEdges("r-failed-again", isoOnlyB))
		}},
		{"caseBaseRepository.RecordProposal", func(t *testing.T) {
			f.a.recordProposal(casebase.Proposal{ProposalID: isoProposal, ReleaseID: "r-failed-again", NodeID: isoOnlyB},
				openPullRequest(isoProposal, "svc", 8, promotedAtOf(3)))
		}},
		{"caseBaseRepository.RecordPullRequestOutcome", func(t *testing.T) {
			f.a.recordOutcome(casebase.PullRequestOutcome{
				ProposalID: isoProposal, ReleaseID: "r-failed-again", Service: "svc", Outcome: "merged",
				ClosedAt: promotedAtOf(3), ResolvedNodeIDs: []string{isoOnlyB},
				Edits: []casebase.EditOutcome{{Path: "models/only_b.sql", TargetNodeID: isoOnlyB, Diff: "d"}},
			})
			assert.Equal(t, int64(2), f.a.editedEdges(isoProposal))
		}},
		{"unitOfWork.Processed", func(t *testing.T) {
			done, err := f.a.uow.Processed(ctx, f.topologyMarker(tenant()))
			require.NoError(t, err)
			assert.False(t, done, "B processed the message, A did not")
			done, err = f.a.uow.Processed(ctx, f.topologyMarker(f.b.tenant))
			require.NoError(t, err)
			assert.True(t, done)
		}},
		{"unitOfWork.Write", func(t *testing.T) {
			err := f.a.uow.Write(ctx, f.topologyMarker(tenant()), true, func(context.Context, uow.Tx) error {
				f.wroteA = true
				return nil
			})
			require.NoError(t, err, "B's export lease does not pause A's writes")
			assert.True(t, f.wroteA, "B's marker does not skip A's write")
		}},
		{"exportLease.Acquire", func(t *testing.T) {
			require.NoError(t, f.lease.Acquire(ctx, tenant(), ports.LeaseExport, isoHolder, time.Minute),
				"B's lease does not hold A's")
		}},
		{"exportLease.Renew", func(t *testing.T) {
			require.NoError(t, f.lease.Renew(ctx, tenant(), isoHolder, time.Minute))
			err := f.lease.Renew(ctx, f.b.tenant, isoHolder, time.Minute)
			assert.True(t, errors.Is(err, ports.ErrLeaseLost), "A's holder does not hold B's lease")
		}},
		{"exportLease.Release", func(t *testing.T) {
			require.NoError(t, f.lease.Release(ctx, tenant(), isoHolder))
			require.NoError(t, f.lease.Release(ctx, f.b.tenant, isoHolder), "releasing another holder's lease is a no-op")
		}},
	}
}
