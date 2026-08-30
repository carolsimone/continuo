package fixer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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

// fakeReleases answers the sibling-failure check's read of the ORIGINAL
// failing release with a fixed verdict; verdictCalls records the ids it was
// asked for, so a test can prove the original release is read and not a
// shadow. A fixer never submits a release or reads an image tag — that is the
// driver's job — so Submit and ImageTag error if a test forgets to assert
// that and the production code regresses into calling them.
type fakeReleases struct {
	verdict      ports.ShadowVerdict
	verdictErr   error
	verdictCalls []string
}

func (f *fakeReleases) Submit(_ context.Context, _ ports.ShadowSubmission) error {
	return fmt.Errorf("a fixer must never submit a shadow release; the driver does")
}

func (f *fakeReleases) Verdict(_ context.Context, releaseID string) (ports.ShadowVerdict, error) {
	f.verdictCalls = append(f.verdictCalls, releaseID)
	return f.verdict, f.verdictErr
}

func (f *fakeReleases) ImageTag(_ context.Context, _, _ string) (string, error) {
	return "", fmt.Errorf("a fixer must never read an image tag; the driver does")
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
    reads:
      orders: select id from analytics.orders
    output_columns:
      - name: revenue
`

const correctedYAML = `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    reads:
      orders: select id from analytics.orders
    output_columns:
      - name: revenue_total
`

// siblingYAML is the second node declared in the same contract directory: not
// the node under repair, but packaged and re-validated alongside it.
const siblingYAML = `nodes:
  - schema: analytics
    table: other
    reads:
      customers: select id from analytics.customers
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
	write("services/service-py/contracts/other.yml", siblingYAML)
	write("services/service-py/scripts/py_daily_kpis.py", "print('hi')\n")
	return root
}

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
	// The real repofs adapter answers both contract ports, so a test cannot
	// pass a search and an inspection that disagree about a document's shape.
	locator := repofs.NewLocator(testLogger())
	pkgr := &fakePackager{merged: []byte("merged: contract\n")}
	arts := &fakeArtifacts{}
	rel := &fakeReleases{}
	svc := Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{{
			Files: []ports.ProposedFile{
				{Path: "services/service-py/contracts/py_daily_kpis.yml", Content: correctedYAML},
			},
			Rationale: "declared the column the script produces", Confidence: "high", Model: "test-model",
		}}},
		Evidence:          fakeEvidence{data: map[string]string{"s3://log": "runner log body"}},
		Sanitizer:         fakeSanitizer{},
		Artifacts:         arts,
		Logger:            testLogger(),
		Upstream:          &fakeUpstream{},
		Precedents:        &fakePrecedents{},
		CandidateSource:   &fakeCandidateSource{src: ports.CandidateSource{RawCode: `{"output_columns":[]}`, Runtime: "python"}},
		Archive:           arch,
		ContractLocator:   locator,
		ContractInspector: locator,
		Packager:          pkgr,
		Releases:          rel,
		PriorAttempts:     &fakeAttempts{},
		SQLDialect:        "postgres",
	}
	return svc, arch, pkgr, rel, arts
}

// --- tests ------------------------------------------------------------------

// TestPythonValidation_HappyPath walks the whole lane end to end and pins the
// property a reordering would break: the contract directory is packaged only
// after the model's files are written into it, never from the still-failing
// tree. The merged contract comes back as Result.ShadowContract for the
// driver to submit and verify — this lane never uploads or submits anything
// itself — and the edit names the node it repairs.
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
	require.Equal(t, siblingYAML, call.sawOnDisk["other.yml"],
		"a file the model did not return must be left exactly as the repository holds it")

	// The merged contract is returned in memory for the driver, not uploaded.
	require.Equal(t, []byte("merged: contract\n"), r.ShadowContract)

	// The sibling check read the ORIGINAL failing release, not a shadow.
	require.Equal(t, []string{"rel-1"}, rel.verdictCalls)

	// The proposal is complete and carries the edit; the driver decides
	// whether it survives a shadow release.
	p := r.Proposal
	require.Equal(t, proposal.StatusProposed, p.Status)
	require.True(t, p.SourceResolved)
	require.Equal(t, "declared the column the script produces", p.Rationale)
	require.Equal(t, proposal.ConfidenceHigh, p.Confidence)
	require.Equal(t, "test-model", p.Model)
	require.Equal(t, "o/demo", p.Repo)
	require.Equal(t, "deadbeef", p.CommitSHA)
	require.Len(t, p.Edits, 1)
	require.Equal(t, "services/service-py/contracts/py_daily_kpis.yml", p.Edits[0].Path)
	require.Equal(t, "analytics.py_daily_kpis", p.Edits[0].TargetNodeID)
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
	svc, arch, pkgr, _, _ := pythonSvc(t, root)
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
			svc, _, pkgr, _, _ := pythonSvc(t, root)
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

			// The tree is untouched: the declaring file still holds the failing
			// contract, so a later attempt starts from the real source.
			b, rerr := os.ReadFile(filepath.Join(root, "services", "service-py", "contracts", "py_daily_kpis.yml")) //nolint:gosec // G304: a path under the test's own temp tree
			require.NoError(t, rerr)
			require.Equal(t, declaringYAML, string(b))
		})
	}
}

// TestPythonValidation_NoFiles_Fails verifies that a model that returned no
// files at all is a failed attempt, not a packaged contract.
func TestPythonValidation_NoFiles_Fails(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, _, _ := pythonSvc(t, root)
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{Confidence: "high"}}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
	require.NotEmpty(t, r.Proposal.Rationale,
		"an operator must see why the attempt failed, not an unexplained failed row")
	require.Empty(t, pkgr.calls)
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
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status)

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
	svc, _, _, _, _ := pythonSvc(t, root)
	svc.Upstream = &fakeUpstream{err: fmt.Errorf("orchestrator down")}
	svc.Precedents = &fakePrecedents{err: fmt.Errorf("orchestrator down")}
	svc.PriorAttempts = &fakeAttempts{err: fmt.Errorf("db down")}
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status)
	require.NotEmpty(t, r.ShadowContract)
}

// TestPythonValidation_TransientBundleError_Redelivers verifies that a
// non-404 object-storage failure returns the error so the trigger is
// redelivered, rather than proposing a fix from evidence it could not read.
func TestPythonValidation_TransientBundleError_Redelivers(t *testing.T) {
	root := pythonRepoTree(t)
	svc, arch, _, _, _ := pythonSvc(t, root)
	svc.CandidateSource = &fakeCandidateSource{err: fmt.Errorf("connection reset")}

	_, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.Error(t, err)
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

// TestFor_ValidationDispatchesOnNodeType verifies that a python-model node's
// validation failure reaches the python lane, a python-csv node's reaches its
// own dedicated lane, and every dbt shape — including a trigger carrying no
// node type at all — keeps reaching today's validation fixer.
func TestFor_ValidationDispatchesOnNodeType(t *testing.T) {
	py, err := For("validation", "python-model")
	require.NoError(t, err)
	require.IsType(t, pythonValidationFixer{}, py)

	csv, err := For("validation", "python-csv")
	require.NoError(t, err)
	require.IsType(t, csvValidationFixer{}, csv)

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
// collaborator which can fail transiently — the checkout fetch and the
// packaging CLI — surfaces its error so the trigger is redelivered.
// Swallowing either would record an attempt that consumed one of the three
// the failure is allowed, while nothing was ever proposed for the driver to
// verify.
func TestPythonValidation_TransientFailuresRedeliver(t *testing.T) {
	boom := fmt.Errorf("boom")
	cases := map[string]func(*fakeArchive, *fakePackager){
		"checkout fetch": func(a *fakeArchive, _ *fakePackager) { a.err = boom },
		"packaging":      func(_ *fakeArchive, p *fakePackager) { p.err = boom },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			svc, arch, pkgr, _, _ := pythonSvc(t, pythonRepoTree(t))
			breakIt(arch, pkgr)

			r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
			require.ErrorIs(t, err, boom)
			require.Equal(t, proposal.Proposal{}, r.Proposal, "a redelivered trigger must record no outcome")
		})
	}
}

// TestPythonValidation_RejectedPackaging_Fails pins the outcome of the one
// failure that looks transient and is not.
//
// When the model returns contract yaml the packaging CLI refuses, the CLI exits
// nonzero every single time it is handed that same input — and a redelivery
// rebuilds exactly that input, because the model's answer is served from the
// idempotency cache keyed on the trigger. Returning the refusal as a transient
// error therefore loops until the stream's poison limit drops the message,
// leaving the attempt in 'generating' with nothing that will ever finish it.
// It is recorded as a failed attempt instead, keeping the CLI's own complaint
// as the rationale so an operator can read why.
func TestPythonValidation_RejectedPackaging_Fails(t *testing.T) {
	svc, arch, pkgr, _, arts := pythonSvc(t, pythonRepoTree(t))
	pkgr.err = fmt.Errorf("continuo-runtime merge: node analytics.py_daily_kpis: output_columns is required: %w",
		ports.ErrContractRejected)

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err, "a contract the packaging tool refuses is settled, not retryable")
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "output_columns is required",
		"the tool's own complaint is the evidence the next attempt is shown")
	require.Empty(t, r.ShadowContract, "nothing was packaged, so there is no contract to hand the driver")
	require.Empty(t, arts.written, "a contract that could not be packaged writes no artifacts")
	require.Equal(t, 1, arch.cleanups, "the checkout is released even when the attempt fails")
}

// TestPythonValidation_UnusableTrigger_Skips verifies that a trigger this lane
// can never act on stops with the reason recorded instead of being redelivered
// forever. A trigger naming no service has no artifact location for its edits;
// a repository or commit that no longer exists cannot be fetched however many
// times the message comes back.
func TestPythonValidation_UnusableTrigger_Skips(t *testing.T) {
	t.Run("no service", func(t *testing.T) {
		svc, arch, _, _, _ := pythonSvc(t, pythonRepoTree(t))
		in := pythonInput()
		in.Service = ""

		r, err := pythonValidationFixer{}.Propose(context.Background(), svc, in)
		require.NoError(t, err)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
		require.Contains(t, r.Proposal.Rationale, "service")
		require.Equal(t, 0, arch.calls)
	})

	t.Run("repository or commit gone", func(t *testing.T) {
		svc, arch, _, _, _ := pythonSvc(t, pythonRepoTree(t))
		arch.err = ports.ErrSourceNotFound

		r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
		require.NoError(t, err)
		require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
		require.NotEmpty(t, r.Proposal.Rationale)
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
	svc, _, pkgr, _, arts := pythonSvc(t, root)
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
	require.Empty(t, arts.written, "a refused answer writes no artifacts")

	// The checkout is untouched, so a later attempt starts from the real source.
	b, rerr := os.ReadFile(filepath.Join(root, "services", "service-py", "contracts", "py_daily_kpis.yml")) //nolint:gosec // G304: a path under the test's own temp tree
	require.NoError(t, rerr)
	require.Equal(t, declaringYAML, string(b))
}

// TestPythonValidation_AnswerThatChangesNodeIdentity_Fails covers the one way
// this lane can produce a confident WRONG answer.
//
// The fix is packaged from the whole contract directory and verified by a real
// release, so the release only ever judges the nodes the packaged contract
// still declares. An answer that DELETES or RENAMES the failing node therefore
// removes the very thing under test: the release has nothing left to reject, it
// reaches "validated", and the edit that deleted a broken node is recorded as a
// verified fix and offered to a human as a pull request. The same holds for a
// sibling declared beside it — an answer that quietly re-identifies one changes
// what production builds under cover of repairing something else.
//
// So every node the answer's own files declared before it was applied must
// still be declared afterwards, with the fields that say WHICH node it is
// (schema, table, script, owner, schedule, criticality) untouched. Anything
// else ends the attempt as failed, naming what moved.
func TestPythonValidation_AnswerThatChangesNodeIdentity_Fails(t *testing.T) {
	const target = "services/service-py/contracts/py_daily_kpis.yml"
	const sibling = "services/service-py/contracts/other.yml"

	cases := map[string]struct {
		files []ports.ProposedFile
		want  string
	}{
		"the failing node is deleted": {
			files: []ports.ProposedFile{{Path: target, Content: "nodes: []\n"}},
			want:  "analytics.py_daily_kpis",
		},
		"the failing node is renamed": {
			files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_daily_kpis_v2
    script: scripts/py_daily_kpis.py
    output_columns:
      - name: revenue_total
`}},
			want: "analytics.py_daily_kpis",
		},
		"the failing node is pointed at another script": {
			files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis_v2.py
    output_columns:
      - name: revenue_total
`}},
			want: "script",
		},
		"a sibling in the same directory is renamed": {
			files: []ports.ProposedFile{
				{Path: target, Content: correctedYAML},
				{Path: sibling, Content: "nodes:\n  - schema: analytics\n    table: other_renamed\n"},
			},
			want: "analytics.other",
		},
		"a returned file is not parseable contract yaml": {
			files: []ports.ProposedFile{{Path: target, Content: "nodes:\n\t- schema: analytics\n"}},
			want:  target,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			root := pythonRepoTree(t)
			svc, _, pkgr, _, arts := pythonSvc(t, root)
			svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{Files: tc.files, Confidence: "high"}}}

			r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
			require.NoError(t, err)
			require.Equal(t, proposal.StatusFailed, r.Proposal.Status,
				"an answer that re-identifies a node must never be verified as a fix")
			require.Contains(t, r.Proposal.Rationale, tc.want,
				"a failed attempt must record what the answer moved")
			require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
			require.Empty(t, arts.written, "a refused answer writes no artifacts")
		})
	}
}

// TestPythonValidation_AnswerAddsExplicitDefaultKind_Verifies covers the
// companion case to the kind-flip check above: declaringYAML, like every
// legacy contract, omits "kind:" entirely, and both the runtime parsers and
// this locator default an absent kind to "python-model". An answer that
// writes that default explicitly is a no-op normalization, not an identity
// change, and must reach verification exactly like any other accepted fix —
// the locator defaulting absent Kind is what keeps this from reading as
// kind "" -> "python-model" and being refused by identityBreach.
func TestPythonValidation_AnswerAddsExplicitDefaultKind_Verifies(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, _, _ := pythonSvc(t, root)
	const answerWithExplicitKind = `nodes:
  - schema: analytics
    table: py_daily_kpis
    kind: python-model
    script: scripts/py_daily_kpis.py
    reads:
      orders: select id from analytics.orders
    output_columns:
      - name: revenue_total
`
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files:     []ports.ProposedFile{{Path: "services/service-py/contracts/py_daily_kpis.yml", Content: answerWithExplicitKind}},
		Rationale: "declared the column the script produces", Confidence: "high", Model: "test-model",
	}}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status,
		"writing the same default kind explicitly must be proposed like any other accepted fix")
	require.Len(t, pkgr.calls, 1)
	require.NotEmpty(t, r.ShadowContract)
}

// TestPythonValidation_AnswerRedeclaringTheNodeElsewhere_Fails covers the
// variant the before/after comparison of the answer's own files cannot see: the
// declaring file is left alone and the node is declared a SECOND time in a new
// file. Nothing was removed or renamed, so every identity the answer touched
// still holds — but the packaged directory now declares one node twice, which
// is a contract no release can validate and a repository state the next attempt
// could not search either.
func TestPythonValidation_AnswerRedeclaringTheNodeElsewhere_Fails(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, _, _ := pythonSvc(t, root)
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		Files: []ports.ProposedFile{
			{Path: "services/service-py/contracts/copy.yml", Content: correctedYAML},
		},
		Confidence: "high",
	}}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusFailed, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "analytics.py_daily_kpis")
	require.Empty(t, pkgr.calls)
}

// TestPythonValidation_AnswerThatDropsADeclaredRead_Fails covers the cheapest
// way to make a shadow release pass without repairing anything.
//
// The validation runner bind-checks the reads a contract declares, one by one.
// Delete the read that could not bind and there is nothing left to check: the
// release validates, the answer is recorded as a proven fix, and a human is
// offered a pull request that deletes the declaration of a read the node's
// script still performs — which this lane cannot see, because it edits the
// contract yaml and never the script. The verdict would be true of what was
// verified and false of what was proposed.
//
// So a read the entry declared before the answer must still be declared after
// it. Correcting a read's SQL, or adding another one, stays open: both leave
// the runner something to check and are how a contract is genuinely repaired.
func TestPythonValidation_AnswerThatDropsADeclaredRead_Fails(t *testing.T) {
	const target = "services/service-py/contracts/py_daily_kpis.yml"
	const sibling = "services/service-py/contracts/other.yml"

	refused := map[string]struct {
		files []ports.ProposedFile
		want  string
	}{
		"the failing node's reads are removed entirely": {
			files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    output_columns:
      - name: revenue_total
`}},
			want: "orders",
		},
		"the failing node's read is renamed": {
			files: []ports.ProposedFile{{Path: target, Content: `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    reads:
      orders_v2: select id from analytics.orders
    output_columns:
      - name: revenue_total
`}},
			want: "orders",
		},
		"a sibling in the same directory loses its read": {
			files: []ports.ProposedFile{
				{Path: target, Content: correctedYAML},
				{Path: sibling, Content: "nodes:\n  - schema: analytics\n    table: other\n"},
			},
			want: "customers",
		},
	}

	for name, tc := range refused {
		t.Run(name, func(t *testing.T) {
			root := pythonRepoTree(t)
			svc, _, pkgr, _, arts := pythonSvc(t, root)
			svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{Files: tc.files, Confidence: "high"}}}

			r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
			require.NoError(t, err)
			require.Equal(t, proposal.StatusFailed, r.Proposal.Status,
				"an answer that deletes a declared read must never be verified as a fix")
			require.Contains(t, r.Proposal.Rationale, tc.want,
				"a failed attempt must name the read the answer dropped")
			require.Empty(t, pkgr.calls, "a refused answer must never be packaged")
			require.Empty(t, arts.written, "a refused answer writes no artifacts")
		})
	}

	allowed := map[string][]ports.ProposedFile{
		"the read's SQL is corrected": {{Path: target, Content: `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    reads:
      orders: select id from analytics.orders_v2
    output_columns:
      - name: revenue_total
`}},
		"another read is added beside it": {{Path: target, Content: `nodes:
  - schema: analytics
    table: py_daily_kpis
    script: scripts/py_daily_kpis.py
    reads:
      orders: select id from analytics.orders
      customers: select id from analytics.customers
    output_columns:
      - name: revenue_total
`}},
	}

	for name, files := range allowed {
		t.Run(name, func(t *testing.T) {
			root := pythonRepoTree(t)
			svc, _, pkgr, _, _ := pythonSvc(t, root)
			svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{Files: files, Confidence: "high"}}}

			r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
			require.NoError(t, err)
			require.Equal(t, proposal.StatusProposed, r.Proposal.Status,
				"a fix that keeps every declared read must be proposed")
			require.Len(t, pkgr.calls, 1)
			require.NotEmpty(t, r.ShadowContract)
		})
	}
}

// TestPythonValidation_SiblingFailureInSameDirectory_Skips covers the case a
// single-node view of the world cannot see.
//
// The fix is packaged from the WHOLE contract directory, but the classifier
// emits one trigger per failing node. So when two nodes declared in one
// directory both fail, each attempt's shadow release re-runs both and is
// rejected by the one it did not fix — however correct the fix is. The
// rejection is then recorded as the next attempt's evidence, naming a node the
// prompt never shows the model, so every attempt ends the same way and the
// per-failure budget is spent on full validation releases that could not have
// passed, each holding the single global release slot. The attempt ends up
// front instead, naming the node that makes it unverifiable.
func TestPythonValidation_SiblingFailureInSameDirectory_Skips(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, rel, arts := pythonSvc(t, root)
	llm := svc.LLM.(*fakeLLM)

	// other.yml sits in the same contracts/ directory as the failing node's
	// own declaring file, so packaging one packages both.
	rel.verdict = ports.ShadowVerdict{Terminal: true, NodeErrors: map[string]string{
		"analytics.py_daily_kpis": "column revenue_total does not exist",
		"analytics.other":         "column country does not exist",
	}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)

	require.Equal(t, proposal.StatusSkipped, r.Proposal.Status)
	require.Contains(t, r.Proposal.Rationale, "analytics.other",
		"the rationale must name the node that makes this fix unverifiable")
	require.Equal(t, []string{"rel-1"}, rel.verdictCalls,
		"the sibling check reads the ORIGINAL failing release, not a shadow")
	require.Equal(t, 0, llm.calls, "no model call is worth making for an unverifiable fix")
	require.Empty(t, pkgr.calls)
	require.Empty(t, r.ShadowContract, "no release slot is spent on a shadow that cannot pass")
	require.Empty(t, arts.written)
}

// TestPythonValidation_FailureElsewhereDoesNotBlock is the control: a node that
// also failed the release but is NOT declared in this contract directory —
// another python service, or a dbt model that no contract yaml declares at all
// — is not packaged by this fix and so cannot reject its shadow release. The
// heal must proceed.
func TestPythonValidation_FailureElsewhereDoesNotBlock(t *testing.T) {
	root := pythonRepoTree(t)
	otherDir := filepath.Join(root, "services", "service-two", "contracts")
	require.NoError(t, os.MkdirAll(otherDir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(otherDir, "z.yml"),
		[]byte("nodes:\n  - schema: analytics\n    table: z\n"), 0o600))

	svc, _, pkgr, rel, _ := pythonSvc(t, root)
	rel.verdict = ports.ShadowVerdict{Terminal: true, NodeErrors: map[string]string{
		"analytics.py_daily_kpis": "column revenue_total does not exist",
		"analytics.z":             "declared in another service's contract directory",
		"model.shop.orders":       "no contract yaml declares a dbt model",
	}}

	r, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, r.Proposal.Status)
	require.Len(t, pkgr.calls, 1)
	require.NotEmpty(t, r.ShadowContract)
}

// TestPythonValidation_UnreadableVerdictIsTransient pins that a release read
// which fails is not answered by guessing. The sibling check decides whether a
// shadow release can mean anything at all, so an unanswerable read ends the
// call with an error — the driver redelivers.
func TestPythonValidation_UnreadableVerdictIsTransient(t *testing.T) {
	root := pythonRepoTree(t)
	svc, _, pkgr, rel, _ := pythonSvc(t, root)
	rel.verdictErr = fmt.Errorf("release-controller unreachable")

	_, err := pythonValidationFixer{}.Propose(context.Background(), svc, pythonInput())
	require.Error(t, err)
	require.Contains(t, err.Error(), "release-controller unreachable")
	require.Empty(t, pkgr.calls)
}
