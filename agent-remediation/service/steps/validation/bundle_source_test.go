package validation_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	loc := &fakeLocator{filePath: "graph/x.sql", service: "svc"}

	out := gather(t, newBundleSource(bundle, &fakeSource{}, loc),
		failure(func(f *remediation.Failure) { f.Target.FilePath = ""; f.Target.Service = "" }), remediation.NewEvidence())

	cs := candidateSource(t, out)
	assert.Equal(t, "graph/x.sql", cs.FilePath)
	assert.Equal(t, "svc", cs.Service)
	assert.Equal(t, []string{"model.svc.a"}, loc.calls)
}

// A node the graph does not know is a permanent miss: no location, so the
// cluster is declined with the reason.
func TestCodeBundleSource_ANodeTheGraphDoesNotKnowDeclines(t *testing.T) {
	bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("select 1")}}
	loc := &fakeLocator{}

	out := gather(t, newBundleSource(bundle, &fakeSource{}, loc),
		failure(func(f *remediation.Failure) { f.Target.FilePath = "" }), remediation.NewEvidence())

	assert.True(t, out.Declined, "a source no fix can reach is not offered to the model")
	assert.Contains(t, out.Decline, "model.svc.a")
	assert.Contains(t, out.Decline, "file path")
	assert.Equal(t, []string{"model.svc.a"}, loc.calls)
	assert.Empty(t, out.Evidence)
}

func TestCodeBundleSource_ANonDbtBundleEntryDeclinesWithItsReason(t *testing.T) {
	for _, runtime := range []string{ports.RuntimePython, ""} {
		t.Run("runtime "+runtime, func(t *testing.T) {
			bundle := &fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": {RawCode: `{"node":1}`, Runtime: runtime}}}
			src := &fakeSource{files: map[string]string{"services/svc/models/a.sql": "select 1"}}

			out := gather(t, newBundleSource(bundle, src, &fakeLocator{}), failure(nil), remediation.NewEvidence())

			assert.True(t, out.Declined)
			assert.Equal(t, `node "model.svc.a" runtime "`+runtime+`": candidate source is not a dbt model`, out.Decline)
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

// A node whose source, file path, service or service repository path is
// unknown has no edit a fix could make, so the step declines it with the reason
// instead of handing the model a call that cannot end in an edit.
func TestCodeBundleSource_DeclinesWithoutALocation(t *testing.T) {
	cases := []struct {
		name   string
		mod    func(*remediation.Failure)
		bundle *fakeBundle
		src    *fakeSource
		reason string
	}{
		{"no file path", func(f *remediation.Failure) { f.Target.FilePath = "" }, &fakeBundle{}, &fakeSource{},
			"the failing node model.svc.a has no file path, so there is no source file to edit"},
		{"no file path although the bundle holds the source", func(f *remediation.Failure) { f.Target.FilePath = "" },
			&fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("select 1")}}, &fakeSource{},
			"the failing node model.svc.a has no file path, so there is no source file to edit"},
		{"no service", func(f *remediation.Failure) { f.Target.Service = "" }, &fakeBundle{}, &fakeSource{},
			"the failing node model.svc.a names no service, so there is no source file to edit"},
		{"a service with no repository path", func(f *remediation.Failure) { f.Target.Service = "ghost" }, &fakeBundle{}, &fakeSource{},
			`service "ghost" has no repository path mapping`},
		{"a source in neither the bundle nor the repository", nil, &fakeBundle{}, &fakeSource{},
			"the failing node model.svc.a's source is in neither the release's code bundle nor the repository at the trigger's commit"},
		{"a file the repository does not have", nil, &fakeBundle{}, &fakeSource{err: ports.ErrSourceNotFound},
			"the failing node model.svc.a's source is in neither the release's code bundle nor the repository at the trigger's commit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := gather(t, newBundleSource(c.bundle, c.src, &fakeLocator{}), failure(c.mod), remediation.NewEvidence())

			assert.True(t, out.Declined)
			assert.Equal(t, c.reason, out.Decline)
			assert.Empty(t, out.Evidence)
		})
	}
}

// A lookup or read that failed says nothing about the node, and would not fail
// on every try, so it is an error for the runner to retry and not a decline.
func TestCodeBundleSource_ATransientErrorIsReturnedNotDeclined(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*remediation.Failure)
		src  *fakeSource
		loc  *fakeLocator
		want string
	}{
		{"a failed lookup of the location", func(f *remediation.Failure) { f.Target.FilePath = "" }, &fakeSource{}, &fakeLocator{err: errBoom},
			"locate model.svc.a"},
		{"a failed repository read", nil, &fakeSource{err: errBoom}, &fakeLocator{},
			"read services/svc/models/a.sql"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := newBundleSource(&fakeBundle{}, c.src, c.loc).
				Gather(context.Background(), step.ContextInput{Failure: failure(c.mod), Evidence: remediation.NewEvidence()})

			require.ErrorIs(t, err, errBoom)
			assert.ErrorContains(t, err, c.want)
			assert.False(t, out.Declined)
			assert.Empty(t, out.Evidence)
		})
	}
}

func TestCodeBundleSource_ALocatedSourceIsOffered(t *testing.T) {
	out := gather(t, newBundleSource(&fakeBundle{nodes: map[string]ports.CandidateSource{"model.svc.a": dbtEntry("select 1")}}, &fakeSource{}, &fakeLocator{}),
		failure(nil), remediation.NewEvidence())

	assert.Equal(t, remediation.CandidateSource{Source: "select 1", Origin: "bundle", FilePath: "models/a.sql", Service: "svc"}, candidateSource(t, out))
}

func TestCodeBundleSource_NoNodeTypeAndANonSQLPathDeclines(t *testing.T) {
	src := &fakeSource{files: map[string]string{"services/svc/models/a.py": "print()"}}

	out := gather(t, newBundleSource(&fakeBundle{}, src, &fakeLocator{}),
		failure(func(f *remediation.Failure) { f.Target.NodeType = ""; f.Target.FilePath = "models/a.py" }), remediation.NewEvidence())

	assert.True(t, out.Declined)
	assert.Equal(t, `node "model.svc.a" path "models/a.py": candidate source is not a dbt model`, out.Decline)
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

// A repository fallback file over the read cap cannot be shown to the model: it
// is no source from the repository, as a missing file is, so the cluster is
// declined with the reason a node with no source gets and the step does not
// fail on every retry.
func TestCodeBundleSource_AnOversizedFallbackFileIsNoSource(t *testing.T) {
	var logs bytes.Buffer
	src := &fakeSource{err: fmt.Errorf("github get p@abc123: source file exceeds %d bytes: %w", ports.MaxSourceBytes, ports.ErrSourceTooLarge)}
	s := validation.NewCodeBundleSource(&fakeBundle{}, src, &fakeLocator{}, repoPaths, slog.New(slog.NewTextHandler(&logs, nil)))

	out := gather(t, s, failure(nil), remediation.NewEvidence())

	assert.True(t, out.Declined)
	assert.Equal(t, "the failing node model.svc.a's source is in neither the release's code bundle nor the repository at the trigger's commit", out.Decline)
	assert.Empty(t, out.Evidence)
	assert.Equal(t, []string{"services/svc/models/a.sql"}, src.reads)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "services/svc/models/a.sql", "the log names the file")
	assert.Contains(t, logs.String(), fmt.Sprint(ports.MaxSourceBytes), "the log names the cap")
}

// A read the repository refuses for another reason, such as a token without
// permission, stays an error for the runner to retry.
func TestCodeBundleSource_AForbiddenFallbackReadStaysAStepError(t *testing.T) {
	forbidden := errors.New("github get p@abc123: status 403: Resource not accessible by integration")

	_, err := newBundleSource(&fakeBundle{}, &fakeSource{err: forbidden}, &fakeLocator{}).
		Gather(context.Background(), step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.ErrorIs(t, err, forbidden)
	assert.NotErrorIs(t, err, ports.ErrSourceTooLarge)
}
