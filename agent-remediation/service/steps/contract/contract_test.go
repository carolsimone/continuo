package contract_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
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

// fakeArchive hands out a real temporary directory per fetch and removes it
// when the step calls cleanup, so a test can see whether a checkout survives.
type fakeArchive struct {
	t        *testing.T
	err      error
	fetches  []fetchCall
	dirs     []string
	cleanups int
}

type fetchCall struct{ repo, ref string }

func (f *fakeArchive) Fetch(_ context.Context, repo, ref string) (string, func(), error) {
	f.fetches = append(f.fetches, fetchCall{repo, ref})
	if f.err != nil {
		return "", nil, f.err
	}
	dir, err := os.MkdirTemp(f.t.TempDir(), "checkout")
	require.NoError(f.t, err)
	f.dirs = append(f.dirs, dir)
	return dir, func() {
		f.cleanups++
		require.NoError(f.t, os.RemoveAll(dir))
	}, nil
}

var _ ports.RepoArchive = (*fakeArchive)(nil)

// requireNoCheckoutLeft fails when any directory the archive handed out is
// still on disk or a cleanup is missing.
func (f *fakeArchive) requireNoCheckoutLeft() {
	f.t.Helper()
	require.Equal(f.t, len(f.dirs), f.cleanups, "every checkout fetched must be cleaned up exactly once")
	for _, d := range f.dirs {
		_, err := os.Stat(d)
		require.True(f.t, os.IsNotExist(err), "checkout %s must be removed", d)
	}
}

// fakeLocator answers Locate by "schema.table" and records every call.
type fakeLocator struct {
	located map[string]ports.Located
	errs    map[string]error
	calls   []locateCall
}

type locateCall struct{ root, schema, table string }

func (f *fakeLocator) Locate(root, schema, table string) (ports.Located, error) {
	f.calls = append(f.calls, locateCall{root, schema, table})
	key := schema + "." + table
	if err := f.errs[key]; err != nil {
		return ports.Located{}, err
	}
	l, ok := f.located[key]
	if !ok {
		return ports.Located{}, ports.ErrNodeNotDeclared
	}
	return l, nil
}

var _ ports.ContractLocator = (*fakeLocator)(nil)

// fakeReleases serves the failing nodes of the original release.
type fakeReleases struct {
	failing  map[string]string
	err      error
	releases []string
}

func (f *fakeReleases) ImageTag(context.Context, string, string) (string, error) {
	return "", errors.New("not used")
}

func (f *fakeReleases) FailingNodes(_ context.Context, releaseID string) (map[string]string, error) {
	f.releases = append(f.releases, releaseID)
	return f.failing, f.err
}

var _ ports.ReleaseReader = (*fakeReleases)(nil)

// fakeBundle serves code bundle entries by node id and records every read.
type fakeBundle struct {
	nodes map[string]ports.CandidateSource
	err   error
	calls []bundleCall
}

type bundleCall struct{ uri, node, release string }

func (f *fakeBundle) NodeSource(_ context.Context, uri, node, release string) (ports.CandidateSource, error) {
	f.calls = append(f.calls, bundleCall{uri, node, release})
	if f.err != nil {
		return ports.CandidateSource{}, f.err
	}
	s, ok := f.nodes[node]
	if !ok {
		return ports.CandidateSource{}, ports.ErrNotFound
	}
	return s, nil
}

var _ ports.CandidateSourceReader = (*fakeBundle)(nil)

// fakeEvidence serves bodies by URI and records every fetch.
type fakeEvidence struct {
	bodies map[string]string
	errs   map[string]error
	calls  []string
}

func (f *fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	f.calls = append(f.calls, uri)
	if err := f.errs[uri]; err != nil {
		return "", err
	}
	b, ok := f.bodies[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return b, nil
}

var _ ports.EvidenceReader = (*fakeEvidence)(nil)

// fakeAttempts lists the recorded attempts and records every filter.
type fakeAttempts struct {
	views   []proposal.View
	err     error
	filters []repository.ProposalFilter
}

func (f *fakeAttempts) List(_ context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	f.filters = append(f.filters, filter)
	return f.views, f.err
}

var _ repository.AttemptLister = (*fakeAttempts)(nil)

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

const (
	nodeID   = "py.fx.rates"
	repoName = "org/repo"
	commit   = "abc123"
)

// theLocated is where the contract of the failing node lives.
var theLocated = ports.Located{
	YAMLPath:    "svc/contracts/fx.yaml",
	YAMLText:    "nodes:\n  - table: rates\n",
	ContractDir: "svc/contracts",
	RepoRoot:    "svc",
}

// failure is a validation failure of a python node, as the trigger carries it.
func failure(mod func(*remediation.Failure)) remediation.Failure {
	f := remediation.Failure{
		Trigger: remediation.Trigger{
			Source:        "validation",
			ReleaseID:     "rel-1",
			Repo:          repoName,
			CommitSHA:     commit,
			CodeBundleURI: "s3://bundle/rel-1",
		},
		Target: remediation.TriggerNode{
			NodeID:         nodeID,
			NodeType:       "python-node",
			ErrorSignature: "sig-1",
			ErrorExcerpt:   "column amount missing",
			DBTLogURI:      "s3://logs/rel-1/a.log",
			Service:        "svc",
		},
		Attempt: 2,
	}
	if mod != nil {
		mod(&f)
	}
	return f
}

func contractFileEvidence() remediation.Evidence {
	return remediation.NewEvidence().With(remediation.KeyContractFile, remediation.ContractFile{
		Schema:      "fx",
		Table:       "rates",
		RepoRoot:    theLocated.RepoRoot,
		ContractDir: theLocated.ContractDir,
		YAMLPath:    theLocated.YAMLPath,
		YAMLText:    theLocated.YAMLText,
	})
}
