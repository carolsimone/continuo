package validation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/validation"
)

func ownDiff(t *testing.T, out step.ContextOutput) remediation.Text {
	t.Helper()
	require.False(t, out.Declined)
	d, ok := out.Evidence[remediation.KeyOwnChangeDiff].(remediation.Text)
	require.True(t, ok, "the step provides own_change_diff")
	return d
}

func withSource(src string) remediation.Evidence {
	return remediation.NewEvidence().With(remediation.KeyCodeBundleSource, remediation.CandidateSource{Source: src})
}

func TestOwnChangeDiff_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := validation.NewOwnChangeDiff(nil, nil).Meta()
	assert.Equal(t, "own_change_diff", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeyCodeBundleSource}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyOwnChangeDiff}, m.Provides)
}

func TestOwnChangeDiff_IsTheDiffFromThePromotedVersion(t *testing.T) {
	versions := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.svc.a": {RawCode: "select 1"}}}

	out := gather(t, validation.NewOwnChangeDiff(versions, discardLogger()), failure(nil), withSource("select 2"))

	want := proposal.ComputeUnifiedDiff("select 1", "select 2", "model.svc.a")
	assert.Equal(t, remediation.Text(want), ownDiff(t, out))
	assert.Equal(t, []string{"model.svc.a"}, versions.calls)
}

func TestOwnChangeDiff_IsEmptyWhenThereIsNothingToDiff(t *testing.T) {
	cases := []struct {
		name     string
		versions *fakeVersions
		source   string
		wantRead bool
	}{
		{"a new node", &fakeVersions{}, "select 2", true},
		{"an unreadable version", &fakeVersions{errs: map[string]error{"model.svc.a": errBoom}}, "select 2", true},
		{"no resolved source", &fakeVersions{versions: map[string]ports.CurrentVersion{"model.svc.a": {RawCode: "select 1"}}}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := gather(t, validation.NewOwnChangeDiff(c.versions, discardLogger()), failure(nil), withSource(c.source))

			assert.Empty(t, ownDiff(t, out))
			assert.Equal(t, c.wantRead, len(c.versions.calls) > 0)
		})
	}
}

func TestOwnChangeDiff_ASmallDiffIsNotMarked(t *testing.T) {
	versions := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.svc.a": {RawCode: "select 1"}}}

	out := gather(t, validation.NewOwnChangeDiff(versions, discardLogger()), failure(nil), withSource("select 2"))

	assert.NotContains(t, string(ownDiff(t, out)), "diff truncated")
}

func TestOwnChangeDiff_RefusesEvidenceWithNoCodeBundleSource(t *testing.T) {
	_, err := validation.NewOwnChangeDiff(&fakeVersions{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.Error(t, err)
}

func TestOwnChangeDiff_ComparesTwoCleanedTexts(t *testing.T) {
	stored := "select '[redacted:email]' as owner\nselect 1"
	versions := &fakeVersions{versions: map[string]ports.CurrentVersion{"model.svc.a": {RawCode: stored}}}

	out := gather(t, validation.NewOwnChangeDiff(versions, discardLogger()), failure(nil), withSource("select 'ops@acme.com' as owner\nselect 2"))

	want := proposal.ComputeUnifiedDiff(stored, "select '[redacted:email]' as owner\nselect 2", "model.svc.a")
	assert.Equal(t, remediation.Text(want), ownDiff(t, out), "catalog's redaction of the stored version never shows as a change")

	unchanged := gather(t, validation.NewOwnChangeDiff(versions, discardLogger()), failure(nil), withSource("select 'ops@acme.com' as owner\nselect 1"))
	assert.Empty(t, ownDiff(t, unchanged), "a source that differs only where catalog redacted has no change")
}
