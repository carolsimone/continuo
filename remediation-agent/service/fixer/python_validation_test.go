package fixer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// --- fakes for the ports the python lane adds -------------------------------

// fakeArchive hands out a pre-built tree as the repository checkout and counts
// how often the caller released it, so a test can prove the extracted tree is
// always cleaned up.
type fakeArchive struct {
	root      string
	err       error
	calls     int
	cleanups  int
	gotRepo   string
	gotCommit string
}

func (f *fakeArchive) Fetch(_ context.Context, repo, ref string) (string, func(), error) {
	f.calls++
	f.gotRepo, f.gotCommit = repo, ref
	if f.err != nil {
		return "", nil, f.err
	}
	return f.root, func() { f.cleanups++ }, nil
}

// packagerCall records one Merge invocation, including the contract directory's
// contents as they were on disk at the moment it ran — the only way to prove
// the tree was patched with the model's answer before it was packaged.
type packagerCall struct {
	contractDir string
	repoRoot    string
	service     string
	dialect     string
	sawOnDisk   map[string]string
}

// fakePackager records every Merge and returns a fixed merged document.
type fakePackager struct {
	merged []byte
	err    error
	calls  []packagerCall
}

func (f *fakePackager) Merge(_ context.Context, contractDir, repoRoot, service, dialect string) ([]byte, error) {
	seen := map[string]string{}
	_ = filepath.WalkDir(contractDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // a fake reading a fixture tree reports what it could read
		}
		b, rerr := os.ReadFile(path) //nolint:gosec // test fixture path
		if rerr == nil {
			rel, _ := filepath.Rel(contractDir, path)
			seen[filepath.ToSlash(rel)] = string(b)
		}
		return nil
	})
	f.calls = append(f.calls, packagerCall{
		contractDir: contractDir, repoRoot: repoRoot,
		service: service, dialect: dialect, sawOnDisk: seen,
	})
	if f.err != nil {
		return nil, f.err
	}
	return f.merged, nil
}

// fakeReleases records the two distinct id spaces the gateway is called with:
// ImageTag reads the ORIGINAL failing release, Submit posts the SHADOW one. It
// also checks, at submission time, that the merged contract is already in
// object storage under the shadow release's own key — release-controller reads
// that object as soon as the submission is accepted, so a submission that
// precedes the upload is a real ordering defect, not a stylistic one.
type fakeReleases struct {
	tag           string
	tagErr        error
	submitErr     error
	imageTagCalls [][2]string // (releaseID, service) per call
	submissions   []ports.ShadowSubmission
	artifacts     *fakeArtifacts
	artifactKey   func(releaseID string) string
	submitSawKey  []bool
}

func (f *fakeReleases) Submit(_ context.Context, s ports.ShadowSubmission) error {
	seen := false
	if f.artifacts != nil && f.artifactKey != nil {
		_, seen = f.artifacts.written[f.artifactKey(s.ReleaseID)]
	}
	f.submitSawKey = append(f.submitSawKey, seen)
	f.submissions = append(f.submissions, s)
	return f.submitErr
}

func (f *fakeReleases) Verdict(context.Context, string) (ports.ShadowVerdict, error) {
	return ports.ShadowVerdict{}, fmt.Errorf("Verdict must not be called by the fixer")
}

func (f *fakeReleases) ImageTag(_ context.Context, releaseID, service string) (string, error) {
	f.imageTagCalls = append(f.imageTagCalls, [2]string{releaseID, service})
	return f.tag, f.tagErr
}

// fakeAttempts returns a fixed set of prior proposal rows and records the
// filter it was asked for.
type fakeAttempts struct {
	views  []proposal.View
	err    error
	filter repository.ProposalFilter
}

func (f *fakeAttempts) List(_ context.Context, filter repository.ProposalFilter) ([]proposal.View, error) {
	f.filter = filter
	return f.views, f.err
}

// --- fixture ----------------------------------------------------------------

const declaringYAML = `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    output_columns:
      - name: revenue
`

const correctedYAML = `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    output_columns:
      - name: revenue_total
`

// pythonRepoTree writes a repository checkout holding one python service whose
// contracts directory declares the failing node, and returns its root.
func pythonRepoTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(root, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	}
	write("services/service-py/contracts/py_daily_kpis.yml", declaringYAML)
	write("services/service-py/contracts/other.yml", "nodes:\n  - schema: analytics\n    table: other\n")
	write("services/service-py/scripts/py_daily_kpis.py", "print('hi')\n")
	return root
}

const wantShadowID = "shadow-rel-1-analytics.py_daily_kpis-a1"
const wantArtifactKey = "svc-py/" + wantShadowID + "/contract.yaml"

// pythonInput is the trigger a rejected python node produces.
func pythonInput() Input {
	return Input{
		Source: "validation", ReleaseID: "rel-1", NodeID: "analytics.py_daily_kpis",
		NodeType: "python-model", Service: "svc-py", Repo: "o/demo", CommitSHA: "deadbeef",
		ErrorExcerpt: "column revenue_total missing", ErrorSignature: "sig-1",
		DBTLogURI: "s3://log", CodeBundleURI: "s3://bundle", Attempt: 1,
	}
}

// pythonSvc wires every collaborator for a python contract fix happy path.
func pythonSvc(t *testing.T, root string) (Services, *fakeArchive, *fakePackager, *fakeReleases, *fakeArtifacts) {
	t.Helper()
	arch := &fakeArchive{root: root}
	pkgr := &fakePackager{merged: []byte("merged: contract\n")}
	arts := &fakeArtifacts{}
	rel := &fakeReleases{
		tag: "ghcr.io/o/service-py:v9", artifacts: arts,
		artifactKey: func(id string) string { return "svc-py/" + id + "/contract.yaml" },
	}
	svc := Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{{
			Files: []ports.ProposedFile{
				{Path: "services/service-py/contracts/py_daily_kpis.yml", Content: correctedYAML},
			},
			Rationale: "declared the column the script produces", Confidence: "high", Model: "test-model",
		}}},
		Evidence:        fakeEvidence{data: map[string]string{"s3://log": "runner log body"}},
		Sanitizer:       fakeSanitizer{},
		Artifacts:       arts,
		Logger:          testLogger(),
		Upstream:        &fakeUpstream{},
		Precedents:      &fakePrecedents{},
		CandidateSource: &fakeCandidateSource{src: ports.CandidateSource{RawCode: `{"output_columns":[]}`, Runtime: "python"}},
		Archive:         arch,
		Packager:        pkgr,
		Releases:        rel,
		PriorAttempts:   &fakeAttempts{},
		SQLDialect:      "postgres",
	}
	return svc, arch, pkgr, rel, arts
}

// --- tests ------------------------------------------------------------------

// TestPythonValidation_HappyPath walks the whole lane end to end and pins the
// two things a reordering would break: the tree is packaged only after the
// model's files are written into it, and the merged contract is in object
// storage before the shadow release that will read it is submitted. It also
// pins the two distinct release-id spaces — the image tag is read from the
// ORIGINAL failing release, while the submission runs under the shadow's own
// id — which nothing else in this service exercises together.
func TestPythonValidation_HappyPath(t *testing.T) {
	root := pythonRepoTree(t)
	svc, arch, pkgr, rel, arts := pythonSvc(t, root)

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)

	// The checkout is fetched at the failing commit and always released.
	require.Equal(t, 1, arch.calls)
	require.Equal(t, "o/demo", arch.gotRepo)
	require.Equal(t, "deadbeef", arch.gotCommit)
	require.Equal(t, 1, arch.cleanups)

	// Packaging runs once, over the declaring file's own directory, with the
	// service's application root (the parent of that directory) as repo root.
	require.Len(t, pkgr.calls, 1)
	call := pkgr.calls[0]
	require.Equal(t, filepath.Join(root, "services", "service-py", "contracts"), call.contractDir)
	require.Equal(t, filepath.Join(root, "services", "service-py"), call.repoRoot)
	require.Equal(t, "svc-py", call.service)
	require.Equal(t, "postgres", call.dialect)

	// What the packager saw on disk is the model's answer, not the failing
	// tree: packaging the unpatched checkout would verify nothing.
	require.Equal(t, correctedYAML, call.sawOnDisk["py_daily_kpis.yml"])
	require.Equal(t, "nodes:\n  - schema: analytics\n    table: other\n", call.sawOnDisk["other.yml"],
		"a file the model did not return must be left exactly as the repository holds it")

	// The merged document is uploaded under the shadow release's own key.
	require.Equal(t, "merged: contract\n", arts.written[wantArtifactKey])

	// Image tag comes from the ORIGINAL failing release id.
	require.Equal(t, [][2]string{{"rel-1", "svc-py"}}, rel.imageTagCalls)

	// The submission runs under the SHADOW release id, reusing that tag, and
	// happens only once the merged contract is already in object storage.
	require.Len(t, rel.submissions, 1)
	require.Equal(t, ports.ShadowSubmission{
		ReleaseID: wantShadowID, Service: "svc-py",
		ImageTag: "ghcr.io/o/service-py:v9", Repo: "o/demo", CommitSHA: "deadbeef",
	}, rel.submissions[0])
	require.Equal(t, []bool{true}, rel.submitSawKey,
		"the merged contract must be uploaded before the shadow release is submitted")

	// The proposal awaits verification and carries the edit.
	p := r.Proposal
	require.Equal(t, proposal.StatusVerifying, p.Status)
	require.Equal(t, wantShadowID, p.ShadowReleaseID)
	require.True(t, p.SourceResolved)
	require.Equal(t, "declared the column the script produces", p.Rationale)
	require.Equal(t, proposal.ConfidenceHigh, p.Confidence)
	require.Equal(t, "test-model", p.Model)
	require.Equal(t, "o/demo", p.Repo)
	require.Equal(t, "deadbeef", p.CommitSHA)
	require.Len(t, p.Edits, 1)
	require.Equal(t, "services/service-py/contracts/py_daily_kpis.yml", p.Edits[0].Path)
	require.Equal(t, p.Edits[0].Path, p.FilePath)
	require.Equal(t, correctedYAML, arts.written["proposed-fix/rel-1/analytics.py_daily_kpis/attempt-1/edit-0.content"])
	require.Contains(t, arts.written["proposed-fix/rel-1/analytics.py_daily_kpis/attempt-1/edit-0.diff"], "+      - name: revenue_total")
}

// TestPythonValidation_NodeNotDeclared_Skips verifies that a node no contract
// file declares is recorded as skipped with the reason kept as the rationale —
// an operator must be able to see why the heal stopped — and that nothing
// downstream of the search runs.
func TestPythonValidation_NodeNotDeclared_Skips(t *testing.T) {
	root := pythonRepoTree(t)
	svc, arch, pkgr, rel, _ := pythonSvc(t, root)
	llm := &fakeLLM{}
	svc.LLM = llm

	in := pythonInput()
	in.NodeID = "analytics.not_here"

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, in)
	require.NoError(t, err)
	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "not declared")
	require.Equal(t, 0, llm.calls)
	require.Empty(t, pkgr.calls)
	require.Empty(t, rel.submissions)
	require.Equal(t, 1, arch.cleanups)
}

// TestPythonValidation_AmbiguousDeclaration_Skips verifies that two files
// declaring the same node stop the heal with the ambiguity recorded, rather
// than the search silently picking one and packaging a fix into the wrong file.
func TestPythonValidation_AmbiguousDeclaration_Skips(t *testing.T) {
	root := pythonRepoTree(t)
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "services", "service-py", "contracts", "dup.yml"),
		[]byte(declaringYAML), 0o600))
	svc, _, pkgr, _, _ := pythonSvc(t, root)

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "more than one")
	require.Empty(t, pkgr.calls)
}

// TestPythonValidation_PathOutsideContractDir_Fails verifies that a model
// answer naming a file outside the contract directory it was shown is refused.
// The fixer writes the answer straight onto a checkout, so an unbounded path is
// how a bad answer would edit an unrelated service — or escape the clone.
func TestPythonValidation_PathOutsideContractDir_Fails(t *testing.T) {
	for name, badPath := range map[string]string{
		"another service":  "services/service-other/contracts/x.yml",
		"parent traversal": "services/service-py/contracts/../../../etc/passwd",
		"absolute":         "/etc/passwd",
	} {
		t.Run(name, func(t *testing.T) {
			root := pythonRepoTree(t)
			svc, _, pkgr, rel, _ := pythonSvc(t, root)
			svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
				Files:      []ports.ProposedFile{{Path: badPath, Content: "nodes: []\n"}},
				Confidence: "high",
			}}}

			r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
			require.NoError(t, err)
			require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
			require.Contains(t, r.Proposal.Rationale, badPath,
				"a failed attempt must record why, naming the path that was refused")
			require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
			require.Empty(t, rel.submissions)

			// The tree is untouched: the declaring file still holds the failing
			// contract, so a later attempt starts from the real source.
			b, rerr := os.ReadFile(filepath.Join(root, "services", "service-py", "contracts", "py_daily_kpis.yml")) //nolint:gosec // G304: a path under the test's own temp tree
			require.NoError(t, rerr)
			require.Equal(t, declaringYAML, string(b))
		})
	}
}

// TestPythonValidation_NoFiles_Fails verifies that a model that returned no
// files at all is a failed attempt, not a submitted shadow release.
func TestPythonValidation_NoFiles_Fails(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, rel, _ := pythonSvc(t, root)
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{Confidence: "high"}}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
	require.NotEmpty(t, r.Proposal.Rationale,
		"an operator must see why the attempt failed, not an unexplained failed row")
	require.Empty(t, pkgr.calls)
	require.Empty(t, rel.submissions)
}

// TestPythonValidation_SecondAttempt_PromptCarriesPriorEvidence verifies the
// property that makes retrying worthwhile: the second attempt's prompt shows
// what the first attempt changed and the error its shadow release came back
// with. Without it every attempt would re-propose the same rejected change.
func TestPythonValidation_SecondAttempt_PromptCarriesPriorEvidence(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, _, _, _ := pythonSvc(t, root)
	llm := &fakeLLM{queue: []ports.ProposeResult{{
		Files:      []ports.ProposedFile{{Path: "services/service-py/contracts/py_daily_kpis.yml", Content: correctedYAML}},
		Confidence: "high",
	}}}
	svc.LLM = llm
	attempts := &fakeAttempts{views: []proposal.View{
		{
			Attempt: 1, Status: proposal.StatusFailed,
			VerifyError: "shadow rejected: revenue_total still missing",
			Edits:       []proposal.FileEdit{{Path: "services/service-py/contracts/py_daily_kpis.yml", DiffURI: "s3://diff-1"}},
		},
		// The row for the attempt now in flight must not be fed back to the
		// model as if it were a previous one.
		{Attempt: 2, Status: proposal.StatusGenerating},
	}}
	svc.PriorAttempts = attempts
	svc.Evidence = fakeEvidence{data: map[string]string{
		"s3://log":    "runner log body",
		"s3://diff-1": "-      - name: revenue\n+      - name: revenue_total",
	}}

	in := pythonInput()
	in.Attempt = 2

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, in)
	require.NoError(t, err)
	require.Equal(t, proposal.StatusVerifying, r.Proposal.Status)
	require.Equal(t, "shadow-rel-1-analytics.py_daily_kpis-a2", r.Proposal.ShadowReleaseID)

	require.Equal(t, repository.ProposalFilter{
		ReleaseID: "rel-1", Source: "validation", NodeID: "analytics.py_daily_kpis",
	}, attempts.filter)

	require.Len(t, llm.requests, 1)
	user := llm.requests[0].User
	require.Contains(t, user, "shadow rejected: revenue_total still missing")
	require.Contains(t, user, "+      - name: revenue_total")
	require.NotContains(t, user, "Attempt 2", "the in-flight attempt is not prior evidence for itself")
}

// TestPythonValidation_EvidenceDegradesNeverBlocks verifies that an
// unavailable graph read, an unavailable prior-attempt read, and a missing
// bundle entry each cost the prompt a section but never the heal.
func TestPythonValidation_EvidenceDegradesNeverBlocks(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, _, rel, _ := pythonSvc(t, root)
	svc.Upstream = &fakeUpstream{err: fmt.Errorf("orchestrator down")}
	svc.Precedents = &fakePrecedents{err: fmt.Errorf("orchestrator down")}
	svc.PriorAttempts = &fakeAttempts{err: fmt.Errorf("db down")}
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusVerifying, r.Proposal.Status)
	require.Len(t, rel.submissions, 1)
}

// TestPythonValidation_TransientBundleError_Redelivers verifies that a
// non-404 object-storage failure returns the error so the trigger is
// redelivered, rather than proposing a fix from evidence it could not read.
func TestPythonValidation_TransientBundleError_Redelivers(t *testing.T) {
	root := pythonRepoTree(t)
	svc, arch, _, rel, _ := pythonSvc(t, root)
	svc.CandidateSource = &fakeCandidateSource{err: fmt.Errorf("connection reset")}

	_, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.Error(t, err)
	require.Empty(t, rel.submissions)
	require.Equal(t, 1, arch.cleanups, "the checkout is released even when the attempt fails")
}

// TestPythonValidation_MalformedNodeID_Skips verifies that a node id from
// which no schema and table can be read stops with a recorded reason rather
// than searching the tree for an empty name.
func TestPythonValidation_MalformedNodeID_Skips(t *testing.T) {
	root := pythonRepoTree(t)
	svc, arch, _, _, _ := pythonSvc(t, root)

	in := pythonInput()
	in.NodeID = "py_daily_kpis"

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, in)
	require.NoError(t, err)
	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.NotEmpty(t, r.Proposal.Rationale)
	require.Equal(t, 0, arch.calls, "a node id that names no schema and table is decided before any fetch")
}

// TestWriteEditArtifacts_TwoEditsOfOneAttemptDoNotCollide verifies that the
// per-edit artifact keys are unique within an attempt. A shared key would make
// the second edit silently overwrite the first, and the proposal would then
// point two file entries at one file's content.
func TestWriteEditArtifacts_TwoEditsOfOneAttemptDoNotCollide(t *testing.T) {
	w := &fakeArtifacts{}
	svc := Services{Artifacts: w, Logger: testLogger()}
	in := Input{ReleaseID: "rel-1", NodeID: "analytics.py_daily_kpis"}

	first, err := writeEditArtifacts(context.Background(), svc, in, 2, 0, "contracts/a.yml", "old-a", "new-a")
	require.NoError(t, err)
	second, err := writeEditArtifacts(context.Background(), svc, in, 2, 1, "contracts/b.yml", "old-b", "new-b")
	require.NoError(t, err)

	require.Equal(t, "proposed-fix/rel-1/analytics.py_daily_kpis/attempt-2/edit-0.content", strings.TrimPrefix(first.ContentURI, "s3://bucket/"))
	require.Equal(t, "proposed-fix/rel-1/analytics.py_daily_kpis/attempt-2/edit-1.content", strings.TrimPrefix(second.ContentURI, "s3://bucket/"))
	require.NotEqual(t, first.ContentURI, second.ContentURI)
	require.NotEqual(t, first.DiffURI, second.DiffURI)
	require.Equal(t, "new-a", w.written[strings.TrimPrefix(first.ContentURI, "s3://bucket/")])
	require.Equal(t, "new-b", w.written[strings.TrimPrefix(second.ContentURI, "s3://bucket/")])
	require.Contains(t, w.written[strings.TrimPrefix(first.DiffURI, "s3://bucket/")], "contracts/a.yml")
	require.Equal(t, "contracts/b.yml", second.Path)
}

// TestFor_ValidationDispatchesOnNodeType verifies that a python node's
// validation failure reaches the python lane while every dbt shape — including
// a trigger carrying no node type at all — keeps reaching today's validation
// fixer.
func TestFor_ValidationDispatchesOnNodeType(t *testing.T) {
	py, err := For("validation", "python-model")
	require.NoError(t, err)
	require.IsType(t, pythonValidationFixer{}, py)

	for _, nodeType := range []string{"dbt-model", "dbt-seed", "dbt-snapshot", ""} {
		f, ferr := For("validation", nodeType)
		require.NoError(t, ferr)
		require.IsType(t, validationFixer{}, f, "node type %q must keep using the dbt validation fixer", nodeType)
	}
}

// TestFor_NonValidationSourcesIgnoreNodeType verifies that node type only
// selects a lane for validation failures: a python node reaching any other
// error class keeps the fixer that class already refuses it in.
func TestFor_NonValidationSourcesIgnoreNodeType(t *testing.T) {
	compile, err := For("compile", "python-model")
	require.NoError(t, err)
	require.IsType(t, compileFixer{}, compile)

	seed, err := For("seed_build", "python-model")
	require.NoError(t, err)
	require.IsType(t, seedFixer{}, seed)

	dup, err := For("duplicate_table", "python-model")
	require.NoError(t, err)
	require.IsType(t, duplicateTableFixer{}, dup)
}

// TestPythonValidation_TransientFailuresRedeliver verifies that each
// collaborator which can fail transiently — the checkout fetch, the packaging
// CLI, the image-tag read, and the shadow submission — surfaces its error so
// the trigger is redelivered. Swallowing any of them would record an attempt
// that consumed one of the three the failure is allowed, while nothing was ever
// submitted for verification.
func TestPythonValidation_TransientFailuresRedeliver(t *testing.T) {
	boom := fmt.Errorf("boom")
	cases := map[string]func(*fakeArchive, *fakePackager, *fakeReleases){
		"checkout fetch": func(a *fakeArchive, _ *fakePackager, _ *fakeReleases) { a.err = boom },
		"packaging":      func(_ *fakeArchive, p *fakePackager, _ *fakeReleases) { p.err = boom },
		"image tag":      func(_ *fakeArchive, _ *fakePackager, r *fakeReleases) { r.tagErr = boom },
		"submission":     func(_ *fakeArchive, _ *fakePackager, r *fakeReleases) { r.submitErr = boom },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			svc, arch, pkgr, rel, _ := pythonSvc(t, pythonRepoTree(t))
			breakIt(arch, pkgr, rel)

			r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
			require.ErrorIs(t, err, boom)
			require.Equal(t, proposal.Proposal{}, r.Proposal, "a redelivered trigger must record no outcome")
		})
	}
}

// TestPythonValidation_UnusableTrigger_Skips verifies that a trigger this lane
// can never act on stops with the reason recorded instead of being redelivered
// forever. A trigger naming no service has no artifact location and no release
// to submit under; a repository or commit that no longer exists cannot be
// fetched however many times the message comes back.
func TestPythonValidation_UnusableTrigger_Skips(t *testing.T) {
	t.Run("no service", func(t *testing.T) {
		svc, arch, _, rel, _ := pythonSvc(t, pythonRepoTree(t))
		in := pythonInput()
		in.Service = ""

		r, err := pythonValidationFixer{}.Propose(context.Background(), svc, in)
		require.NoError(t, err)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
		require.Contains(t, r.Proposal.Rationale, "service")
		require.Equal(t, 0, arch.calls)
		require.Empty(t, rel.submissions)
	})

	t.Run("repository or commit gone", func(t *testing.T) {
		svc, arch, _, rel, _ := pythonSvc(t, pythonRepoTree(t))
		arch.err = ports.ErrSourceNotFound

		r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
		require.NoError(t, err)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
		require.NotEmpty(t, r.Proposal.Rationale)
		require.Empty(t, rel.submissions)
	})
}

// TestPythonValidation_DuplicatePath_Fails verifies that an answer naming the
// same file twice is refused. Applying it would leave the last entry's content
// on disk — which is what gets packaged and what the shadow release actually
// verifies — while the recorded edits, and the proposal's own single-file view
// built from edits[0], would describe the first entry instead. A human would
// then approve a change that was never the one validated, which is precisely
// what verifying the real bytes is meant to rule out.
func TestPythonValidation_DuplicatePath_Fails(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, rel, arts := pythonSvc(t, root)
	const target = "services/service-py/contracts/py_daily_kpis.yml"
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{
			{Path: target, Content: "nodes: []\n"},
			{Path: target, Content: correctedYAML},
		},
		Confidence: "high",
	}}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, target)
	require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
	require.Empty(t, rel.submissions)
	require.Empty(t, arts.written, "a refused answer writes no artifacts")

	// The checkout is untouched, so a later attempt starts from the real source.
	b, rerr := os.ReadFile(filepath.Join(root, "services", "service-py", "contracts", "py_daily_kpis.yml")) //nolint:gosec // G304: a path under the test's own temp tree
	require.NoError(t, rerr)
	require.Equal(t, declaringYAML, string(b))
}
