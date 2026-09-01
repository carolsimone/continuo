// File: orchestrator/service/queries/precedent_query_service_test.go
package queries_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePrecedentReader is a hand-rolled fake satisfying queries.PrecedentReader.
type fakePrecedentReader struct {
	views []casebase.PrecedentView
	err   error

	// captured args, for assertions on what the service passed down.
	gotSignature   string
	gotCategory    string
	gotReason      string
	gotLimit       int32
	gotIncludeCode bool
}

func (f *fakePrecedentReader) Precedents(ctx context.Context, signature, category, reason string, limit int32, includeCode bool) ([]casebase.PrecedentView, error) {
	f.gotSignature = signature
	f.gotCategory = category
	f.gotReason = reason
	f.gotLimit = limit
	f.gotIncludeCode = includeCode
	if f.err != nil {
		return nil, f.err
	}
	return f.views, nil
}

func newPrecedentSvc(r *fakePrecedentReader) *queries.PrecedentQueryService {
	return queries.NewPrecedentQueryService(r)
}

func TestPrecedentService_RendersResolutionDiff(t *testing.T) {
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection:        casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl"},
			ResolvingVersion: &codeversion.VersionView{RawCode: "select 2", VersionSeq: 2},
			PriorVersion:     &codeversion.VersionView{RawCode: "select 1", VersionSeq: 1},
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, true)
	require.NoError(t, err)
	require.Len(t, out, 1)

	p := out[0]
	assert.True(t, p.Resolved)
	assert.Contains(t, p.ResolutionDiff, "-select 1")
	assert.Contains(t, p.ResolutionDiff, "+select 2")
	assert.False(t, p.ResolutionDiffTruncated)
}

func TestPrecedentService_IncludeCodeFalseStripsBodies(t *testing.T) {
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection:        casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl", RawCode: ""},
			ResolvingVersion: &codeversion.VersionView{RawCode: "select 2", CompiledCode: "compiled 2", VersionSeq: 2},
			PriorVersion:     &codeversion.VersionView{RawCode: "select 1", VersionSeq: 1},
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, false)
	require.NoError(t, err)
	require.Len(t, out, 1)

	p := out[0]
	assert.Equal(t, "", p.Rejection.RawCode, "the reader already projects raw_code empty when include_code is false")
	require.NotNil(t, p.ResolvingVersion)
	assert.Equal(t, "", p.ResolvingVersion.RawCode)
	assert.Equal(t, "", p.ResolvingVersion.CompiledCode)
	// The diff is rendered from the reader's un-stripped views before the
	// service strips its own copy of ResolvingVersion — it must still show up.
	assert.Contains(t, p.ResolutionDiff, "-select 1")
	assert.Contains(t, p.ResolutionDiff, "+select 2")
}

func TestPrecedentService_ResolvedByEditedProvenanceWithoutOwnTimeline(t *testing.T) {
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection:        casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl"},
			ResolvingVersion: nil, // resolved only via a merged PR's edits
			Edited: []casebase.EditedView{
				{NodeID: "svc.schema.upstream", Path: "models/upstream.sql", Amended: false, Diff: "D1"},
			},
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, true)
	require.NoError(t, err)
	require.Len(t, out, 1)

	p := out[0]
	assert.True(t, p.Resolved, "an edited-provenance entry resolves the precedent even without an own-timeline version")
	assert.Nil(t, p.ResolvingVersion)
	assert.Empty(t, p.ResolutionDiff, "no own-timeline diff when there is no resolving version")
	require.Len(t, p.Edited, 1)
	assert.Equal(t, "svc.schema.upstream", p.Edited[0].NodeID)
	assert.Equal(t, "D1", p.Edited[0].Diff, "a non-amended edit keeps the edge diff verbatim")
	assert.False(t, p.Edited[0].DiffIsShipped, "the fallback edge diff is not the shipped merged truth")
}

// TestPrecedentService_AmendedFallbackDiffIsNotShipped covers an edit amended
// before merge whose merged version has not been promoted yet: no straddle
// (MergedVersion nil), so the service keeps the proposal-time diff and must
// leave DiffIsShipped false — the diff is not the shipped truth.
func TestPrecedentService_AmendedFallbackDiffIsNotShipped(t *testing.T) {
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection: casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl"},
			Edited: []casebase.EditedView{
				{NodeID: "svc.schema.tbl", Path: "models/tbl.sql", Amended: true, Diff: "proposal-diff"},
			},
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, true)
	require.NoError(t, err)
	require.Len(t, out, 1)

	require.Len(t, out[0].Edited, 1)
	assert.Equal(t, "proposal-diff", out[0].Edited[0].Diff, "no straddle: the proposal-time diff is kept")
	assert.False(t, out[0].Edited[0].DiffIsShipped, "an amended edit with no promoted merged version is not shipped")
}

func TestPrecedentService_AmendedEditRendersMergedTruthDiff(t *testing.T) {
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection: casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl"},
			Edited: []casebase.EditedView{
				{
					NodeID: "svc.schema.tbl", Path: "models/tbl.sql", Amended: true,
					Diff:          "proposal-diff", // must be replaced by the merged-truth diff
					MergedPrior:   &codeversion.VersionView{RawCode: "select 1", VersionSeq: 3},
					MergedVersion: &codeversion.VersionView{RawCode: "select 2", VersionSeq: 4},
				},
			},
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, true)
	require.NoError(t, err)
	require.Len(t, out, 1)

	p := out[0]
	assert.True(t, p.Resolved)
	require.Len(t, p.Edited, 1)
	assert.NotEqual(t, "proposal-diff", p.Edited[0].Diff,
		"an amended edit with a straddling version renders the merged-truth diff, not the proposal diff")
	assert.Contains(t, p.Edited[0].Diff, "-select 1")
	assert.Contains(t, p.Edited[0].Diff, "+select 2")
	assert.True(t, p.Edited[0].DiffIsShipped, "a merged-NodeVersion straddle marks the diff as the shipped truth")
}

func TestPrecedentService_ProposalResolvedWithoutDrawnEditsIsResolved(t *testing.T) {
	// A rejection resolved by a merged PR whose EDITED targets were all absent
	// from the graph: the reader surfaces ResolvedByProposal=true but no Edited
	// rows and no own-timeline version. The service must still report Resolved.
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection:          casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl"},
			ResolvingVersion:   nil,
			Edited:             nil,
			ResolvedByProposal: true,
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, true)
	require.NoError(t, err)
	require.Len(t, out, 1)

	p := out[0]
	assert.True(t, p.Resolved, "a proposal-resolved rejection is resolved even with no drawn edits")
	assert.Nil(t, p.ResolvingVersion, "no own-timeline version to show")
	assert.Empty(t, p.Edited, "no fabricated edited provenance when none was drawn")
	assert.Empty(t, p.ResolutionDiff)
}

func TestPrecedentService_UnresolvedHasNoDiff(t *testing.T) {
	r := &fakePrecedentReader{views: []casebase.PrecedentView{
		{
			Rejection:        casebase.Rejection{ReleaseID: "rel-1", NodeID: "svc.schema.tbl"},
			ResolvingVersion: nil,
		},
	}}
	svc := newPrecedentSvc(r)

	out, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", 5, true)
	require.NoError(t, err)
	require.Len(t, out, 1)

	p := out[0]
	assert.False(t, p.Resolved)
	assert.Nil(t, p.ResolvingVersion)
	assert.Empty(t, p.ResolutionDiff)
	assert.False(t, p.ResolutionDiffTruncated)
}

func TestPrecedentService_LimitClamped(t *testing.T) {
	cases := []struct {
		name      string
		reqLimit  int32
		wantLimit int32
	}{
		{"zero defaults to 5", 0, 5},
		{"negative defaults to 5", -1, 5},
		{"within range passes through", 12, 12},
		{"oversized clamps to 20", 999, 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakePrecedentReader{}
			svc := newPrecedentSvc(r)

			_, err := svc.GetPrecedents(context.Background(), "sig-1", "", "", tc.reqLimit, false)
			require.NoError(t, err)
			assert.Equal(t, tc.wantLimit, r.gotLimit)
		})
	}
}
