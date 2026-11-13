package validation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/validation"
)

var repoPaths = map[string]string{"svc": "services/svc"}

func dbtEntry(code string) ports.CandidateSource {
	return ports.CandidateSource{RawCode: code, Runtime: ports.RuntimeDbt}
}

func candidateSource(t *testing.T, out step.ContextOutput) remediation.CandidateSource {
	t.Helper()
	require.False(t, out.Declined)
	cs, ok := out.Evidence[remediation.KeyCodeBundleSource].(remediation.CandidateSource)
	require.True(t, ok, "the step provides code_bundle_source")
	return cs
}

func newBundleSource(b *fakeBundle, src *fakeSource, loc *fakeLocator) *validation.CodeBundleSource {
	return validation.NewCodeBundleSource(b, src, loc, repoPaths, discardLogger())
}

func TestCodeBundleSource_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := validation.NewCodeBundleSource(nil, nil, nil, nil, nil).Meta()
	assert.Equal(t, "code_bundle_source", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyCodeBundleSource}, m.Provides)
}

func TestCodeBundleSource_ReadsTheBundleAtTheTriggersLocation(t *testing.T) {
	bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("select 1")}}
	loc := &fakeLocator{filePath: "graph/x.sql", service: "graph-svc"}

	out := gather(t, newBundleSource(bundle, &fakeSource{}, loc), failure(nil), remediation.NewEvidence())

	assert.Equal(t, remediation.CandidateSource{Source: "select 1", Origin: "bundle", FilePath: "models/a.sql", Service: "svc"}, candidateSource(t, out))
	assert.Equal(t, []bundleCall{{"s3://bundle/rel-1", "model.svc.a", "rel-1"}}, bundle.calls)
	assert.Empty(t, loc.calls, "a trigger that names the location is not located")
}

func TestCodeBundleSource_LocatesANodeTheTriggerDoesNotPlace(t *testing.T) {
	bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("select 1")}}
	loc := &fakeLocator{filePath: "graph/x.sql", service: "graph-svc"}

	out := gather(t, newBundleSource(bundle, &fakeSource{}, loc),
		failure(func(f *remediation.Failure) { f.Target.FilePath = ""; f.Target.Service = "" }), remediation.NewEvidence())

	cs := candidateSource(t, out)
	assert.Equal(t, "graph/x.sql", cs.FilePath)
	assert.Equal(t, "graph-svc", cs.Service)
	assert.Equal(t, []string{"model.svc.a"}, loc.calls)
}

func TestCodeBundleSource_ALocatorErrorBlanksTheLocationAndKeepsTheSource(t *testing.T) {
	bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("select 1")}}
	loc := &fakeLocator{err: errBoom}

	out := gather(t, newBundleSource(bundle, &fakeSource{}, loc),
		failure(func(f *remediation.Failure) { f.Target.FilePath = "" }), remediation.NewEvidence())

	assert.Equal(t, remediation.CandidateSource{Source: "select 1", Origin: "bundle"}, candidateSource(t, out))
}

func TestCodeBundleSource_ANonDbtBundleEntryDeclinesWithNoMessage(t *testing.T) {
	for _, runtime := range []string{ports.RuntimePython, ""} {
		t.Run("runtime "+runtime, func(t *testing.T) {
			bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": {RawCode: `{"node":1}`, Runtime: runtime}}}
			src := &fakeSource{files: map[string]string{"services/svc/models/a.sql": "select 1"}}

			out := gather(t, newBundleSource(bundle, src, &fakeLocator{}), failure(nil), remediation.NewEvidence())

			assert.True(t, out.Declined)
			assert.Empty(t, out.Decline)
			assert.Empty(t, out.Evidence)
			assert.Empty(t, src.reads, "the repository is not read for a bundle entry that is not dbt")
		})
	}
}

func TestCodeBundleSource_FallsBackToTheRepositoryOnAPermanentBundleMiss(t *testing.T) {
	cases := []struct {
		name   string
		bundle *fakeBundle
	}{
		{"a missing entry", &fakeBundle{}},
		{"a dbt entry with no source text", &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("")}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &fakeSource{files: map[string]string{"services/svc/models/a.sql": "select from repo"}}

			out := gather(t, newBundleSource(c.bundle, src, &fakeLocator{}), failure(nil), remediation.NewEvidence())

			assert.Equal(t, remediation.CandidateSource{Source: "select from repo", Origin: "github", FilePath: "models/a.sql", Service: "svc"}, candidateSource(t, out))
			assert.Equal(t, []string{"services/svc/models/a.sql"}, src.reads)
		})
	}
}

func TestCodeBundleSource_ABundleReadErrorIsReturned(t *testing.T) {
	bundle := &fakeBundle{errs: map[string]error{"model.svc.a": errBoom}}

	_, err := newBundleSource(bundle, &fakeSource{}, &fakeLocator{}).
		Gather(context.Background(), step.ContextInput{Failure: failure(nil)})

	require.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "fetch code bundle")
}

func TestCodeBundleSource_AnUnresolvedSourceIsNotADecline(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*remediation.Failure)
		src  *fakeSource
	}{
		{"no file path", func(f *remediation.Failure) { f.Target.FilePath = "" }, &fakeSource{}},
		{"no service", func(f *remediation.Failure) { f.Target.Service = "" }, &fakeSource{}},
		{"a service with no repository path", func(f *remediation.Failure) { f.Target.Service = "ghost" }, &fakeSource{}},
		{"a failed repository read", nil, &fakeSource{err: errBoom}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := gather(t, newBundleSource(&fakeBundle{}, c.src, &fakeLocator{}), failure(c.mod), remediation.NewEvidence())

			cs := candidateSource(t, out)
			assert.Empty(t, cs.Source)
			assert.Empty(t, cs.Origin)
		})
	}
}

func TestCodeBundleSource_NoNodeTypeAndANonSQLPathDeclines(t *testing.T) {
	src := &fakeSource{files: map[string]string{"services/svc/models/a.py": "print()"}}

	out := gather(t, newBundleSource(&fakeBundle{}, src, &fakeLocator{}),
		failure(func(f *remediation.Failure) { f.Target.NodeType = ""; f.Target.FilePath = "models/a.py" }), remediation.NewEvidence())

	assert.True(t, out.Declined)
	assert.Empty(t, out.Decline)
	assert.Empty(t, src.reads, "a script is refused without being read")
}

func TestCodeBundleSource_ANodeTypeIsTrustedOverTheExtension(t *testing.T) {
	src := &fakeSource{files: map[string]string{"services/svc/snapshots/a.yml": "snapshot: x"}}

	out := gather(t, newBundleSource(&fakeBundle{}, src, &fakeLocator{}),
		failure(func(f *remediation.Failure) {
			f.Target.NodeType = "dbt-snapshot"
			f.Target.FilePath = "snapshots/a.yml"
		}), remediation.NewEvidence())

	assert.Equal(t, "snapshot: x", candidateSource(t, out).Source)
}
