package validation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/validation"
)

func ancestorDiffs(t *testing.T, out step.ContextOutput) []prompt.ReleaseUpstreamChange {
	t.Helper()
	require.False(t, out.Declined)
	d, ok := out.Evidence[remediation.KeyChangedAncestorDiffs].([]prompt.ReleaseUpstreamChange)
	require.True(t, ok, "the step provides changed_ancestor_diffs")
	return d
}

func withAncestors(as ...remediation.ChangedAncestor) func(*remediation.Failure) {
	return func(f *remediation.Failure) { f.Target.ChangedAncestors = as }
}

func TestChangedAncestorDiffs_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := validation.NewChangedAncestorDiffs(nil, nil, nil, nil).Meta()
	assert.Equal(t, "changed_ancestor_diffs", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyChangedAncestorDiffs}, m.Provides)
}

func TestChangedAncestorDiffs_DiffsEachAncestorsBundleSourceAgainstItsPromotedVersion(t *testing.T) {
	bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.up": dbtEntry("select 2")}}
	versions := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.svc.up": {RawCode: "select 1"}}}
	f := failure(withAncestors(remediation.ChangedAncestor{NodeID: "model.svc.up", Service: "svc", FilePath: "models/up.sql", Depth: 1}))

	out := gather(t, validation.NewChangedAncestorDiffs(bundle, versions, markSanitizer{}, discardLogger()), f, remediation.NewEvidence())

	assert.Equal(t, []prompt.ReleaseUpstreamChange{{
		NodeID: "model.svc.up", Service: "svc", Depth: 1,
		Diff: "<" + proposal.ComputeUnifiedDiff("select 1", "select 2", "model.svc.up") + ">",
	}}, ancestorDiffs(t, out))
	assert.Equal(t, []bundleCall{{"s3://bundle/rel-1", "model.svc.up", "rel-1"}}, bundle.calls)
}

func TestChangedAncestorDiffs_ShowsTheThreeNearestByDepthThenId(t *testing.T) {
	var as []remediation.ChangedAncestor
	for _, a := range []struct {
		id    string
		depth int
	}{{"d", 2}, {"c", 1}, {"e", 3}, {"b", 2}, {"a", 3}, {"f", 1}} {
		as = append(as, remediation.ChangedAncestor{NodeID: a.id, Service: "s-" + a.id, Depth: a.depth})
	}
	f := failure(withAncestors(as...))

	out := gather(t, validation.NewChangedAncestorDiffs(&fakeBundle{}, &fakeVersions{}, markSanitizer{}, discardLogger()), f, remediation.NewEvidence())

	got := ancestorDiffs(t, out)
	var ids []string
	for _, g := range got {
		ids = append(ids, g.NodeID)
	}
	assert.Equal(t, []string{"c", "f", "b"}, ids)
	assert.Equal(t, "d", f.Target.ChangedAncestors[0].NodeID, "the trigger's own list is not reordered")
}

func TestChangedAncestorDiffs_ListsAnAncestorWithNoDiffWhenItsSourceOrVersionIsUnavailable(t *testing.T) {
	bundle := &fakeBundle{
		nodes: map[string]ports.CandidateSource{"new": dbtEntry("select 2"), "bad-version": dbtEntry("select 2")},
		errs:  map[string]error{"unreadable": errBoom},
	}
	versions := &fakeVersions{errs: map[string]error{"bad-version": errBoom}}
	f := failure(withAncestors(
		remediation.ChangedAncestor{NodeID: "missing", Depth: 1},
		remediation.ChangedAncestor{NodeID: "unreadable", Depth: 2},
		remediation.ChangedAncestor{NodeID: "new", Depth: 3},
	))

	out := gather(t, validation.NewChangedAncestorDiffs(bundle, versions, markSanitizer{}, discardLogger()), f, remediation.NewEvidence())

	got := ancestorDiffs(t, out)
	require.Len(t, got, 3)
	for _, g := range got {
		assert.Empty(t, g.Diff, g.NodeID)
	}
}

func TestChangedAncestorDiffs_NoAncestorsProvidesAnEmptyList(t *testing.T) {
	out := gather(t, validation.NewChangedAncestorDiffs(&fakeBundle{}, &fakeVersions{}, markSanitizer{}, discardLogger()), failure(nil), remediation.NewEvidence())

	got := ancestorDiffs(t, out)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}
