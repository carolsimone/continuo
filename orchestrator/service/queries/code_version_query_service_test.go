// File: orchestrator/service/queries/code_version_query_service_test.go
package queries_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCodeVersionReader is a hand-rolled fake satisfying queries.CodeVersionReader.
type fakeCodeVersionReader struct {
	nodeVersions    []codeversion.VersionView
	nodeVersionsErr error

	versionsBySeqFrom codeversion.VersionView
	versionsBySeqTo   codeversion.VersionView
	versionsBySeqErr  error

	ancestors    []codeversion.AncestorVersions
	ancestorsErr error

	unitVersions    map[string][]codeversion.UnitVersionView
	unitVersionsErr error

	unitsForNode    []string
	unitsForNodeErr error

	runExecutions    []codeversion.RunExecution
	runExecutionsErr error

	// captured args, for assertions on what the service passed down.
	gotNodeVersionsLimit  int32
	gotAncestorsDepth     int32
	gotAncestorsSince     time.Time
	gotRunExecutionsLimit int32
	gotUnitVersionsLimit  map[string]int32
}

func (f *fakeCodeVersionReader) NodeVersions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.VersionView, error) {
	f.gotNodeVersionsLimit = limit
	if f.nodeVersionsErr != nil {
		return nil, f.nodeVersionsErr
	}
	return f.nodeVersions, nil
}

func (f *fakeCodeVersionReader) VersionsBySeq(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (codeversion.VersionView, codeversion.VersionView, error) {
	if f.versionsBySeqErr != nil {
		return codeversion.VersionView{}, codeversion.VersionView{}, f.versionsBySeqErr
	}
	return f.versionsBySeqFrom, f.versionsBySeqTo, nil
}

func (f *fakeCodeVersionReader) Ancestors(ctx context.Context, uniqueID string, depth int32, since time.Time) ([]codeversion.AncestorVersions, error) {
	f.gotAncestorsDepth = depth
	f.gotAncestorsSince = since
	if f.ancestorsErr != nil {
		return nil, f.ancestorsErr
	}
	return f.ancestors, nil
}

func (f *fakeCodeVersionReader) UnitVersions(ctx context.Context, unitID string, limit int32) ([]codeversion.UnitVersionView, error) {
	if f.gotUnitVersionsLimit == nil {
		f.gotUnitVersionsLimit = map[string]int32{}
	}
	f.gotUnitVersionsLimit[unitID] = limit
	if f.unitVersionsErr != nil {
		return nil, f.unitVersionsErr
	}
	return f.unitVersions[unitID], nil
}

func (f *fakeCodeVersionReader) UnitsForNode(ctx context.Context, uniqueID string) ([]string, error) {
	if f.unitsForNodeErr != nil {
		return nil, f.unitsForNodeErr
	}
	return f.unitsForNode, nil
}

func (f *fakeCodeVersionReader) RunExecutions(ctx context.Context, uniqueID string, limit int32) ([]codeversion.RunExecution, error) {
	f.gotRunExecutionsLimit = limit
	if f.runExecutionsErr != nil {
		return nil, f.runExecutionsErr
	}
	return f.runExecutions, nil
}

func newCodeVersionSvc(r *fakeCodeVersionReader) *queries.CodeVersionQueryService {
	return queries.NewCodeVersionQueryService(r)
}

// ---- GetUpstreamChanges ----

func TestCodeVersionQueryService_GetUpstreamChanges_CapsAtFiveMostRecentlyChanged(t *testing.T) {
	// The fake returns ancestors already ordered most-recently-changed first,
	// as the port's contract promises; the service must keep the head 5.
	ancestors := make([]codeversion.AncestorVersions, 0, 8)
	for i := 0; i < 8; i++ {
		ancestors = append(ancestors, codeversion.AncestorVersions{
			UniqueID: strings.Repeat("up", 1) + string(rune('a'+i)),
			Depth:    1,
			Versions: []codeversion.VersionView{
				{VersionSeq: 2, RawCode: "new"},
				{VersionSeq: 1, RawCode: "old"},
			},
		})
	}
	r := &fakeCodeVersionReader{ancestors: ancestors}
	svc := newCodeVersionSvc(r)

	changes, err := svc.GetUpstreamChanges(context.Background(), "node-1", 3, time.Time{})
	require.NoError(t, err)
	require.Len(t, changes, 5)
	for i, c := range changes {
		assert.Equal(t, ancestors[i].UniqueID, c.UniqueID, "changes must keep the head (most-recently-changed) order")
	}
}

func TestCodeVersionQueryService_GetUpstreamChanges_DefaultsDepthWhenNonPositive(t *testing.T) {
	r := &fakeCodeVersionReader{}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetUpstreamChanges(context.Background(), "node-1", 0, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, int32(3), r.gotAncestorsDepth)
}

func TestCodeVersionQueryService_GetUpstreamChanges_SincePassesThroughUnmodified(t *testing.T) {
	r := &fakeCodeVersionReader{}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetUpstreamChanges(context.Background(), "node-1", 2, time.Time{})
	require.NoError(t, err)
	assert.True(t, r.gotAncestorsSince.IsZero(), "zero-value since means no time filter")

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = svc.GetUpstreamChanges(context.Background(), "node-1", 2, since)
	require.NoError(t, err)
	assert.Equal(t, since, r.gotAncestorsSince)
}

func TestCodeVersionQueryService_GetUpstreamChanges_SkipsZeroVersionAncestor(t *testing.T) {
	r := &fakeCodeVersionReader{
		ancestors: []codeversion.AncestorVersions{
			{UniqueID: "has-versions", Depth: 1, Versions: []codeversion.VersionView{{VersionSeq: 1, RawCode: "x"}}},
			{UniqueID: "no-versions", Depth: 2, Versions: nil},
		},
	}
	svc := newCodeVersionSvc(r)

	changes, err := svc.GetUpstreamChanges(context.Background(), "node-1", 3, time.Time{})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "has-versions", changes[0].UniqueID)
}

func TestCodeVersionQueryService_GetUpstreamChanges_SingleVersionDiffsFromEmpty(t *testing.T) {
	r := &fakeCodeVersionReader{
		ancestors: []codeversion.AncestorVersions{
			{
				UniqueID: "up-1",
				Depth:    1,
				Versions: []codeversion.VersionView{
					{UniqueID: "up-1", VersionSeq: 1, RawCode: "select 1", SourceHash: "h1"},
				},
			},
		},
	}
	svc := newCodeVersionSvc(r)

	changes, err := svc.GetUpstreamChanges(context.Background(), "node-1", 3, time.Time{})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, codeversion.VersionView{}, changes[0].Diff.From, "single version diffs from an empty VersionView")
	assert.Equal(t, "up-1", changes[0].Diff.To.UniqueID)
	assert.Equal(t, "select 1", changes[0].Diff.To.RawCode)
	assert.True(t, changes[0].Diff.SourceChanged, "whole code is the change when there is no prior version")
}

func TestCodeVersionQueryService_GetUpstreamChanges_ReaderError(t *testing.T) {
	r := &fakeCodeVersionReader{ancestorsErr: errors.New("neo4j down")}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetUpstreamChanges(context.Background(), "node-1", 3, time.Time{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neo4j down")
}

// ---- GetNodeVersionDiff ----

func TestCodeVersionQueryService_GetNodeVersionDiff_OverCapSetsTruncatedAndValidUTF8(t *testing.T) {
	// "é" is a 2-byte rune; a large run of them virtually guarantees the raw
	// byte cap lands mid-rune unless the implementation walks back to a
	// rune-start, which is exactly what this exercises end-to-end.
	big := strings.Repeat("é", 20000) // 40,000 bytes, far past the 8 KiB cap
	r := &fakeCodeVersionReader{
		versionsBySeqFrom: codeversion.VersionView{VersionSeq: 3, RawCode: "", SourceHash: "a"},
		versionsBySeqTo:   codeversion.VersionView{VersionSeq: 5, RawCode: big, SourceHash: "b"},
	}
	svc := newCodeVersionSvc(r)

	diff, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 3, 5)
	require.NoError(t, err)
	assert.True(t, diff.Truncated)
	assert.LessOrEqual(t, len(diff.RawCodeDiff), 8*1024)
	assert.True(t, utf8.ValidString(diff.RawCodeDiff), "truncation must not split a UTF-8 rune")
}

func TestCodeVersionQueryService_GetNodeVersionDiff_IdenticalVersionsEmptyDiffsAllFlagsFalse(t *testing.T) {
	v := codeversion.VersionView{
		VersionSeq: 4,
		RawCode:    "select * from t",
		SourceHash: "h1", SharedCodeHash: "h2", ConfigHash: "h3",
		ConfigJSON: `{"materialized":"table"}`,
	}
	r := &fakeCodeVersionReader{versionsBySeqFrom: v, versionsBySeqTo: v}
	svc := newCodeVersionSvc(r)

	diff, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 4, 4)
	require.NoError(t, err)
	assert.Empty(t, diff.RawCodeDiff)
	assert.Empty(t, diff.ConfigDiff)
	assert.False(t, diff.SourceChanged)
	assert.False(t, diff.SharedCodeChanged)
	assert.False(t, diff.ConfigChanged)
	assert.False(t, diff.Truncated)
}

func TestCodeVersionQueryService_GetNodeVersionDiff_ConfigOnlyChangeSetsConfigChangedAlone(t *testing.T) {
	from := codeversion.VersionView{
		VersionSeq: 1, RawCode: "select 1",
		SourceHash: "same-source", SharedCodeHash: "same-shared", ConfigHash: "hash-a",
		ConfigJSON: `{"materialized":"view"}`,
	}
	to := codeversion.VersionView{
		VersionSeq: 2, RawCode: "select 1",
		SourceHash: "same-source", SharedCodeHash: "same-shared", ConfigHash: "hash-b",
		ConfigJSON: `{"materialized":"table"}`,
	}
	r := &fakeCodeVersionReader{versionsBySeqFrom: from, versionsBySeqTo: to}
	svc := newCodeVersionSvc(r)

	diff, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 1, 2)
	require.NoError(t, err)
	assert.False(t, diff.SourceChanged)
	assert.False(t, diff.SharedCodeChanged)
	assert.True(t, diff.ConfigChanged)
	assert.Empty(t, diff.RawCodeDiff, "source text is identical")
	assert.NotEmpty(t, diff.ConfigDiff)
}

func TestCodeVersionQueryService_GetNodeVersionDiff_KeyOrderAloneProducesNoConfigDiff(t *testing.T) {
	from := codeversion.VersionView{
		VersionSeq: 1, ConfigJSON: `{"a":1,"b":2}`,
	}
	to := codeversion.VersionView{
		VersionSeq: 2, ConfigJSON: `{"b":2,"a":1}`,
	}
	r := &fakeCodeVersionReader{versionsBySeqFrom: from, versionsBySeqTo: to}
	svc := newCodeVersionSvc(r)

	diff, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 1, 2)
	require.NoError(t, err)
	assert.Empty(t, diff.ConfigDiff, "key order alone must not manufacture a diff")
}

func TestCodeVersionQueryService_GetNodeVersionDiff_EmptyConfigJSONTreatedAsEmptyObject(t *testing.T) {
	from := codeversion.VersionView{VersionSeq: 1, ConfigJSON: ""}
	to := codeversion.VersionView{VersionSeq: 2, ConfigJSON: "{}"}
	r := &fakeCodeVersionReader{versionsBySeqFrom: from, versionsBySeqTo: to}
	svc := newCodeVersionSvc(r)

	diff, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 1, 2)
	require.NoError(t, err)
	assert.Empty(t, diff.ConfigDiff)
}

func TestCodeVersionQueryService_GetNodeVersionDiff_MalformedJSONDegradesToRawDiff(t *testing.T) {
	from := codeversion.VersionView{VersionSeq: 1, ConfigJSON: `{not json`}
	to := codeversion.VersionView{VersionSeq: 2, ConfigJSON: `{also not json`}
	r := &fakeCodeVersionReader{versionsBySeqFrom: from, versionsBySeqTo: to}
	svc := newCodeVersionSvc(r)

	diff, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 1, 2)
	require.NoError(t, err, "canonicalization failure must degrade, never fail the request")
	assert.NotEmpty(t, diff.ConfigDiff, "falls back to diffing the raw strings")
}

func TestCodeVersionQueryService_GetNodeVersionDiff_SeqNotFoundReturnsError(t *testing.T) {
	r := &fakeCodeVersionReader{versionsBySeqErr: domain.ErrNodeNotFound}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetNodeVersionDiff(context.Background(), "node-1", 1, 99)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
}

// ---- GetNodeVersions ----

func TestCodeVersionQueryService_GetNodeVersions_EmptyHistoryReturnsEmptySlice(t *testing.T) {
	r := &fakeCodeVersionReader{nodeVersions: []codeversion.VersionView{}}
	svc := newCodeVersionSvc(r)

	versions, err := svc.GetNodeVersions(context.Background(), "known-node", 10)
	require.NoError(t, err)
	assert.Empty(t, versions)
	assert.NotNil(t, versions)
}

func TestCodeVersionQueryService_GetNodeVersions_UnknownNodeReturnsError(t *testing.T) {
	r := &fakeCodeVersionReader{nodeVersionsErr: domain.ErrNodeNotFound}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetNodeVersions(context.Background(), "ghost-node", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
}

func TestCodeVersionQueryService_GetNodeVersions_LimitPassesThrough(t *testing.T) {
	r := &fakeCodeVersionReader{}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetNodeVersions(context.Background(), "node-1", 7)
	require.NoError(t, err)
	assert.Equal(t, int32(7), r.gotNodeVersionsLimit)
}

// ---- GetNodeRunHistory ----

func TestCodeVersionQueryService_GetNodeRunHistory_NonPositiveLimitDefaultsTo20(t *testing.T) {
	r := &fakeCodeVersionReader{}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetNodeRunHistory(context.Background(), "node-1", 0)
	require.NoError(t, err)
	assert.Equal(t, int32(20), r.gotRunExecutionsLimit)

	_, err = svc.GetNodeRunHistory(context.Background(), "node-1", -5)
	require.NoError(t, err)
	assert.Equal(t, int32(20), r.gotRunExecutionsLimit)
}

func TestCodeVersionQueryService_GetNodeRunHistory_LimitCappedAt200(t *testing.T) {
	r := &fakeCodeVersionReader{}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetNodeRunHistory(context.Background(), "node-1", 10000)
	require.NoError(t, err)
	assert.Equal(t, int32(200), r.gotRunExecutionsLimit)
}

func TestCodeVersionQueryService_GetNodeRunHistory_LimitWithinRangePassesThrough(t *testing.T) {
	r := &fakeCodeVersionReader{}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetNodeRunHistory(context.Background(), "node-1", 50)
	require.NoError(t, err)
	assert.Equal(t, int32(50), r.gotRunExecutionsLimit)
}

func TestCodeVersionQueryService_GetNodeRunHistory_EmptyHistoryReturnsEmptySlice(t *testing.T) {
	r := &fakeCodeVersionReader{runExecutions: []codeversion.RunExecution{}}
	svc := newCodeVersionSvc(r)

	runs, err := svc.GetNodeRunHistory(context.Background(), "node-1", 20)
	require.NoError(t, err)
	assert.Empty(t, runs)
}

// ---- GetCodeUnitVersions ----

func TestCodeVersionQueryService_GetCodeUnitVersions_ByUnitIDDirect(t *testing.T) {
	want := []codeversion.UnitVersionView{{UnitID: "macro-a", Checksum: "c1"}}
	r := &fakeCodeVersionReader{unitVersions: map[string][]codeversion.UnitVersionView{"macro-a": want}}
	svc := newCodeVersionSvc(r)

	got, err := svc.GetCodeUnitVersions(context.Background(), "macro-a", "", 10)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestCodeVersionQueryService_GetCodeUnitVersions_ByUniqueIDResolvesUnits(t *testing.T) {
	r := &fakeCodeVersionReader{
		unitsForNode: []string{"macro-a", "macro-b"},
		unitVersions: map[string][]codeversion.UnitVersionView{
			"macro-a": {{UnitID: "macro-a", Checksum: "c1"}},
			"macro-b": {{UnitID: "macro-b", Checksum: "c2"}, {UnitID: "macro-b", Checksum: "c0"}},
		},
	}
	svc := newCodeVersionSvc(r)

	got, err := svc.GetCodeUnitVersions(context.Background(), "", "node-1", 10)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "macro-a", got[0].UnitID)
	assert.Equal(t, "macro-b", got[1].UnitID)
	assert.Equal(t, "macro-b", got[2].UnitID)
}

func TestCodeVersionQueryService_GetCodeUnitVersions_UnknownNodeError(t *testing.T) {
	r := &fakeCodeVersionReader{unitsForNodeErr: domain.ErrNodeNotFound}
	svc := newCodeVersionSvc(r)

	_, err := svc.GetCodeUnitVersions(context.Background(), "", "ghost-node", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
}
