package upstream_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

var errBoom = errors.New("boom")

var repoPaths = map[string]string{"up-svc": "services/up-svc", "down-svc": "services/down-svc"}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// markSanitizer wraps what it is given, so a test can tell sanitized text from
// raw text.
type markSanitizer struct{}

func (markSanitizer) Sanitize(s string) string { return "<" + s + ">" }

var _ ports.LogSanitizer = markSanitizer{}

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

// fakeModel records every request and answers with the scripted result.
type fakeModel struct {
	res  ports.ProposeResult
	err  error
	reqs []prompt.ProposeRequest
}

func (f *fakeModel) Propose(_ context.Context, req prompt.ProposeRequest) (ports.ProposeResult, error) {
	f.reqs = append(f.reqs, req)
	return f.res, f.err
}

var _ step.Model = (*fakeModel)(nil)

const (
	targetID     = "model.up-svc.up"
	targetSource = "select old_name from raw"
	targetFile   = "models/up.sql"
	targetSvc    = "up-svc"
)

// failure is a shared-upstream cluster as the trigger carries it: the changed
// ancestor and two failing descendants, each naming where the candidate
// topology declares the ancestor.
func failure(mod func(*remediation.Failure)) remediation.Failure {
	ancestor := remediation.ChangedAncestor{NodeID: targetID, FilePath: targetFile, Service: targetSvc, Depth: 1}
	members := []remediation.TriggerNode{
		{NodeID: "model.up-svc.m1", ErrorSignature: "sig-1", Category: "validation", Reason: "missing_column", ErrorExcerpt: "column old_name missing", Service: targetSvc, ChangedAncestors: []remediation.ChangedAncestor{ancestor}},
		{NodeID: "model.up-svc.m2", ErrorSignature: "sig-1", ErrorExcerpt: "column old_name missing too", ChangedAncestors: []remediation.ChangedAncestor{ancestor}},
	}
	f := remediation.Failure{
		Trigger: remediation.Trigger{
			Source:        "validation",
			ReleaseID:     "rel-1",
			Repo:          "org/repo",
			CommitSHA:     "abc123",
			CodeBundleURI: "s3://bundle/rel-1",
			Nodes:         members,
		},
		Kind:         workflow.ClusterSharedUpstream,
		TargetNodeID: targetID,
		Target:       remediation.TriggerNode{NodeID: targetID},
		Members:      members,
		Attempt:      2,
	}
	if mod != nil {
		mod(&f)
	}
	return f
}

func dbtEntry(code string) ports.CandidateSource {
	return ports.CandidateSource{RawCode: code, Runtime: ports.RuntimeDbt}
}

func gather(t *testing.T, s step.ContextSource, f remediation.Failure, ev remediation.Evidence) step.ContextOutput {
	t.Helper()
	out, err := s.Gather(context.Background(), step.ContextInput{Failure: f, Evidence: ev})
	require.NoError(t, err)
	return out
}
