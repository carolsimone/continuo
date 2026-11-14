package validation_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

var errBoom = errors.New("boom")

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// markSanitizer wraps what it is given, so a test can tell sanitized text from
// raw text.
type markSanitizer struct{}

func (markSanitizer) Sanitize(s string) string { return "<" + s + ">" }

var _ ports.LogSanitizer = markSanitizer{}

// fakeEvidence serves bodies by URI and records every fetch.
type fakeEvidence struct {
	bodies map[string]string
	err    error
	calls  []string
}

func (f *fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	f.calls = append(f.calls, uri)
	if f.err != nil {
		return "", f.err
	}
	b, ok := f.bodies[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return b, nil
}

var _ ports.EvidenceReader = (*fakeEvidence)(nil)

// fakeBundle serves code bundle entries by node id and records every read.
type fakeBundle struct {
	nodes map[string]ports.CandidateSource
	errs  map[string]error
	calls []bundleCall
}

type bundleCall struct{ uri, node, release string }

func (f *fakeBundle) NodeSource(_ context.Context, uri, node, release string) (ports.CandidateSource, error) {
	f.calls = append(f.calls, bundleCall{uri, node, release})
	if err := f.errs[node]; err != nil {
		return ports.CandidateSource{}, err
	}
	s, ok := f.nodes[node]
	if !ok {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	return s, nil
}

var _ ports.CandidateSourceReader = (*fakeBundle)(nil)

// fakeVersions serves the version each node runs now.
type fakeVersions struct {
	versions map[string]ports.CurrentVersion
	errs     map[string]error
	calls    []string
}

func (f *fakeVersions) CurrentVersion(_ context.Context, id string) (ports.CurrentVersion, bool, error) {
	f.calls = append(f.calls, id)
	if err := f.errs[id]; err != nil {
		return ports.CurrentVersion{}, false, err
	}
	v, ok := f.versions[id]
	return v, ok, nil
}

var _ ports.VersionReader = (*fakeVersions)(nil)

// fakeSource serves repository files and records reads.
type fakeSource struct {
	files map[string]string
	err   error
	reads []string
}

func (f *fakeSource) ReadFile(_ context.Context, _, _, p string) (string, error) {
	f.reads = append(f.reads, p)
	if f.err != nil {
		return "", f.err
	}
	c, ok := f.files[p]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

func (f *fakeSource) ListDir(context.Context, string, string, string) ([]string, error) {
	return nil, errors.New("not used")
}

var _ ports.SourceReader = (*fakeSource)(nil)

type fakeLocator struct {
	filePath, service string
	err               error
	calls             []string
}

func (f *fakeLocator) Locate(_ context.Context, id string) (string, string, error) {
	f.calls = append(f.calls, id)
	return f.filePath, f.service, f.err
}

var _ ports.NodeLocator = (*fakeLocator)(nil)

// fakeArtifacts records every write and answers with a URI built from the key.
type fakeArtifacts struct {
	writes []artifactWrite
	failOn string
}

type artifactWrite struct{ key, body, contentType string }

func (f *fakeArtifacts) Write(_ context.Context, key, body, contentType string) (string, error) {
	if f.failOn != "" && key == f.failOn {
		return "", errBoom
	}
	f.writes = append(f.writes, artifactWrite{key, body, contentType})
	return "s3://artifacts/" + key, nil
}

var _ ports.ArtifactWriter = (*fakeArtifacts)(nil)

// fakeModel records every request and answers with the scripted results in
// order.
type fakeModel struct {
	answers []modelAnswer
	reqs    []prompt.ProposeRequest
}

type modelAnswer struct {
	res ports.ProposeResult
	err error
}

func (f *fakeModel) Propose(_ context.Context, req prompt.ProposeRequest) (ports.ProposeResult, error) {
	f.reqs = append(f.reqs, req)
	a := f.answers[0]
	f.answers = f.answers[1:]
	return a.res, a.err
}

var _ step.Model = (*fakeModel)(nil)

func answers(rs ...ports.ProposeResult) *fakeModel {
	m := &fakeModel{}
	for _, r := range rs {
		m.answers = append(m.answers, modelAnswer{res: r})
	}
	return m
}

// failure is a validation failure of a dbt model, as the trigger carries it.
func failure(mod func(*remediation.Failure)) remediation.Failure {
	f := remediation.Failure{
		Trigger: remediation.Trigger{
			Source:        "validation",
			ReleaseID:     "rel-1",
			Repo:          "org/repo",
			CommitSHA:     "abc123",
			CodeBundleURI: "s3://bundle/rel-1",
		},
		Target: remediation.TriggerNode{
			NodeID:               "model.svc.a",
			NodeType:             "dbt-model",
			ErrorSignature:       "sig-1",
			CandidateArtifactURI: "s3://candidate/a.sql",
			FilePath:             "models/a.sql",
			Service:              "svc",
		},
		Attempt: 2,
	}
	if mod != nil {
		mod(&f)
	}
	return f
}

func gather(t *testing.T, s step.ContextSource, f remediation.Failure, ev remediation.Evidence) step.ContextOutput {
	t.Helper()
	out, err := s.Gather(context.Background(), step.ContextInput{Failure: f, Evidence: ev})
	require.NoError(t, err)
	return out
}
