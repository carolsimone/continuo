package upstream_test

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/upstream"
)

func newTarget(b *fakeBundle, v *fakeVersions, l *fakeLocator) *upstream.Target {
	return upstream.NewTarget(b, l, v, markSanitizer{}, repoPaths, discardLogger())
}

func okBundle() *fakeBundle {
	return &fakeBundle{nodes: map[string]ports.CandidateSource{targetID: dbtEntry(targetSource)}}
}

func gatheredTarget(t *testing.T, out step.ContextOutput) remediation.UpstreamTarget {
	t.Helper()
	require.False(t, out.Declined)
	got, ok := out.Evidence[remediation.KeyUpstreamTarget].(remediation.UpstreamTarget)
	require.True(t, ok, "the step provides upstream_target")
	return got
}

func TestTarget_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := upstream.NewTarget(nil, nil, nil, nil, nil, nil).Meta()
	assert.Equal(t, "upstream_target", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyUpstreamTarget}, m.Provides)
}

func TestTarget_ReadsTheAncestorFromTheBundleAtTheTriggersLocation(t *testing.T) {
	bundle := okBundle()
	loc := &fakeLocator{filePath: "graph/x.sql", service: "down-svc"}
	versions := &fakeVersions{versions: map[string]ports.CurrentVersion{targetID: {RawCode: "select older from raw"}}}

	out := gather(t, newTarget(bundle, versions, loc), failure(nil), remediation.NewEvidence())

	assert.Equal(t, remediation.UpstreamTarget{
		NodeID:        targetID,
		Source:        targetSource,
		FilePath:      targetFile,
		Service:       targetSvc,
		OwnChangeDiff: "<" + proposal.ComputeUnifiedDiff("select older from raw", targetSource, targetID) + ">",
	}, gatheredTarget(t, out))
	assert.Equal(t, []bundleCall{{"s3://bundle/rel-1", targetID, "rel-1"}}, bundle.calls)
	assert.Equal(t, []string{targetID}, versions.calls)
	assert.Empty(t, loc.calls, "a trigger that places the ancestor does not locate it")
}

func TestTarget_LocatesAnAncestorTheTriggerDoesNotPlace(t *testing.T) {
	unplaced := []struct {
		name string
		mod  func(*remediation.Failure)
	}{
		{"no ancestor entry has a location", func(f *remediation.Failure) {
			for i := range f.Trigger.Nodes {
				f.Trigger.Nodes[i].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: targetID}}
			}
		}},
		{"only the file path is given", func(f *remediation.Failure) {
			for i := range f.Trigger.Nodes {
				f.Trigger.Nodes[i].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: targetID, FilePath: targetFile}}
			}
		}},
		{"only the service is given", func(f *remediation.Failure) {
			for i := range f.Trigger.Nodes {
				f.Trigger.Nodes[i].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: targetID, Service: targetSvc}}
			}
		}},
		{"the trigger names another ancestor", func(f *remediation.Failure) {
			for i := range f.Trigger.Nodes {
				f.Trigger.Nodes[i].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: "model.up-svc.other", FilePath: "x.sql", Service: targetSvc}}
			}
		}},
	}
	for _, c := range unplaced {
		t.Run(c.name, func(t *testing.T) {
			loc := &fakeLocator{filePath: "graph/x.sql", service: "down-svc"}

			out := gather(t, newTarget(okBundle(), &fakeVersions{}, loc), failure(c.mod), remediation.NewEvidence())

			got := gatheredTarget(t, out)
			assert.Equal(t, "graph/x.sql", got.FilePath)
			assert.Equal(t, "down-svc", got.Service)
			assert.Equal(t, []string{targetID}, loc.calls)
		})
	}
}

func TestTarget_TheFirstLocatedAncestorEntryWins(t *testing.T) {
	loc := &fakeLocator{}
	out := gather(t, newTarget(okBundle(), &fakeVersions{}, loc), failure(func(f *remediation.Failure) {
		f.Trigger.Nodes[0].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: targetID}}
		f.Trigger.Nodes[1].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: targetID, FilePath: "second.sql", Service: "down-svc"}}
	}), remediation.NewEvidence())

	got := gatheredTarget(t, out)
	assert.Equal(t, "second.sql", got.FilePath)
	assert.Equal(t, "down-svc", got.Service)
	assert.Empty(t, loc.calls)
}

func TestTarget_DeclinesWithTheLanesTexts(t *testing.T) {
	cases := []struct {
		name    string
		bundle  *fakeBundle
		loc     *fakeLocator
		mod     func(*remediation.Failure)
		decline string
	}{
		{
			name:    "no member",
			bundle:  okBundle(),
			loc:     &fakeLocator{},
			mod:     func(f *remediation.Failure) { f.Members = nil },
			decline: "an upstream cluster needs at least one failing member",
		},
		{
			name:    "a bundle miss",
			bundle:  &fakeBundle{},
			loc:     &fakeLocator{},
			decline: "the changed ancestor's source is not in the release's code bundle",
		},
		{
			name:    "a python ancestor",
			bundle:  &fakeBundle{nodes: map[string]ports.CandidateSource{targetID: {RawCode: "{}", Runtime: ports.RuntimePython}}},
			loc:     &fakeLocator{},
			decline: `the changed ancestor model.up-svc.up is not a dbt model (runtime "python")`,
		},
		{
			name:    "an ancestor with no runtime",
			bundle:  &fakeBundle{nodes: map[string]ports.CandidateSource{targetID: {RawCode: "select 1"}}},
			loc:     &fakeLocator{},
			decline: `the changed ancestor model.up-svc.up is not a dbt model (runtime "")`,
		},
		{
			name:   "an ancestor the graph cannot place",
			bundle: okBundle(),
			loc:    &fakeLocator{},
			mod: func(f *remediation.Failure) {
				f.Trigger.Nodes[0].ChangedAncestors, f.Trigger.Nodes[1].ChangedAncestors = nil, nil
			},
			decline: "the changed ancestor model.up-svc.up cannot be located in the promoted graph",
		},
		{
			name:   "a locator error",
			bundle: okBundle(),
			loc:    &fakeLocator{err: errBoom},
			mod: func(f *remediation.Failure) {
				f.Trigger.Nodes[0].ChangedAncestors, f.Trigger.Nodes[1].ChangedAncestors = nil, nil
			},
			decline: "the changed ancestor model.up-svc.up cannot be located in the promoted graph",
		},
		{
			name:   "a location with no service",
			bundle: okBundle(),
			loc:    &fakeLocator{filePath: "x.sql"},
			mod: func(f *remediation.Failure) {
				f.Trigger.Nodes[0].ChangedAncestors, f.Trigger.Nodes[1].ChangedAncestors = nil, nil
			},
			decline: "the changed ancestor model.up-svc.up cannot be located in the promoted graph",
		},
		{
			name:   "a service with no repository path",
			bundle: okBundle(),
			loc:    &fakeLocator{},
			mod: func(f *remediation.Failure) {
				f.Trigger.Nodes[0].ChangedAncestors = []remediation.ChangedAncestor{{NodeID: targetID, FilePath: targetFile, Service: "ghost"}}
			},
			decline: `service "ghost" has no repository path mapping`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := gather(t, newTarget(c.bundle, &fakeVersions{}, c.loc), failure(c.mod), remediation.NewEvidence())

			assert.True(t, out.Declined)
			assert.Equal(t, c.decline, out.Decline)
			assert.Empty(t, out.Evidence)
		})
	}
}

func TestTarget_ADeclineBeforeTheBundleReadReadsNothing(t *testing.T) {
	bundle := okBundle()

	gather(t, newTarget(bundle, &fakeVersions{}, &fakeLocator{}), failure(func(f *remediation.Failure) { f.Members = nil }), remediation.NewEvidence())

	assert.Empty(t, bundle.calls)
}

func TestTarget_ATransientBundleErrorIsReturnedNotDeclined(t *testing.T) {
	bundle := &fakeBundle{errs: map[string]error{targetID: errBoom}}

	out, err := newTarget(bundle, &fakeVersions{}, &fakeLocator{}).
		Gather(context.Background(), step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Equal(t, "fetch code bundle for model.up-svc.up: boom", err.Error())
	assert.False(t, out.Declined)
}

func TestTarget_TheOwnChangeDiffIsEmptyWhenThereIsNothingToDiff(t *testing.T) {
	cases := map[string]*fakeVersions{
		"a new node":            {},
		"an unreadable version": {errs: map[string]error{targetID: errBoom}},
	}
	for name, versions := range cases {
		t.Run(name, func(t *testing.T) {
			out := gather(t, newTarget(okBundle(), versions, &fakeLocator{}), failure(nil), remediation.NewEvidence())

			got := gatheredTarget(t, out)
			assert.Empty(t, got.OwnChangeDiff)
			assert.Equal(t, targetSource, got.Source, "the evidence still carries the ancestor")
		})
	}
}

func TestTarget_TheSourceIsKeptRawAndTheDiffIsSanitized(t *testing.T) {
	versions := &fakeVersions{versions: map[string]ports.CurrentVersion{targetID: {RawCode: "select 1"}}}

	out := gather(t, newTarget(okBundle(), versions, &fakeLocator{}), failure(nil), remediation.NewEvidence())

	got := gatheredTarget(t, out)
	assert.Equal(t, targetSource, got.Source, "the edit's original is the real file")
	assert.True(t, strings.HasPrefix(got.OwnChangeDiff, "<") && strings.HasSuffix(got.OwnChangeDiff, ">"))
}

func TestTarget_CapsALongDiffAtARuneBoundary(t *testing.T) {
	// A diff of two-byte runes cut at 8192 bytes lands inside a rune for one of
	// two neighbouring prefix lengths, so one of these inputs needs the cut
	// backed up.
	for _, pad := range []string{"", "x"} {
		t.Run("pad "+pad, func(t *testing.T) {
			bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{
				targetID: dbtEntry("select '" + pad + strings.Repeat("é", 6000) + "'"),
			}}
			versions := &fakeVersions{versions: map[string]ports.CurrentVersion{targetID: {RawCode: "select 1"}}}

			out := gather(t, newTarget(bundle, versions, &fakeLocator{}), failure(nil), remediation.NewEvidence())

			diff := gatheredTarget(t, out).OwnChangeDiff
			require.True(t, strings.HasSuffix(diff, "\n… (diff truncated)"))
			body := strings.TrimSuffix(diff, "\n… (diff truncated)")
			assert.LessOrEqual(t, len(body), 8192)
			assert.Greater(t, len(body), 8190)
			assert.True(t, utf8.ValidString(body), "the cut does not split a rune")
		})
	}
}
