package fixer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// testLogger returns a discard-output logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 100}))
}

// fakeEvidence returns pre-loaded strings by URI, or an error if set.
type fakeEvidence struct {
	data map[string]string
	err  error
}

func (f fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.data[uri], nil
}

// fakeSanitizer is a pass-through log sanitizer.
type fakeSanitizer struct{}

func (fakeSanitizer) Sanitize(s string) string { return s }

// fakeLLM returns results from a queue (one per Propose call, in order). When
// the queue is exhausted, the last entry is repeated.
type fakeLLM struct {
	queue       []ports.ProposeResult
	errs        []error
	calls       int
	requests    []ports.ProposeRequest // records each request in call order
	lastRequest ports.ProposeRequest   // the most recent request, for a test that only cares about the last call
}

func (f *fakeLLM) Propose(_ context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	f.requests = append(f.requests, req)
	f.lastRequest = req
	i := f.calls
	if i >= len(f.queue) {
		i = len(f.queue) - 1
	}
	var e error
	if i < len(f.errs) {
		e = f.errs[i]
	}
	f.calls++
	return f.queue[i], e
}

// redactingSanitizer replaces a fixed secret with a marker so tests can prove a
// Fixer routed source content through the LogSanitizer seam before the LLM call.
type redactingSanitizer struct{ secret, marker string }

func (s redactingSanitizer) Sanitize(in string) string {
	return strings.ReplaceAll(in, s.secret, s.marker)
}

// fakePrecedents returns precedents from an in-memory signature or
// category+reason index, or an error if set. calls counts every invocation so
// a test can assert a skip path never queries it.
type fakePrecedents struct {
	bySignature map[string][]prompt.Precedent
	byClass     map[string][]prompt.Precedent // key: category + "|" + reason
	err         error
	calls       int
}

func (f *fakePrecedents) Precedents(_ context.Context, q ports.PrecedentQuery) ([]prompt.Precedent, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if q.Signature != "" {
		return f.bySignature[q.Signature], nil
	}
	return f.byClass[q.Category+"|"+q.Reason], nil
}

// fakeLocator returns a fixed file path and service name, or an error, for
// NodeLocator.Locate.
type fakeLocator struct {
	filePath, serviceName string
	err                   error
}

func (f fakeLocator) Locate(_ context.Context, _ string) (string, string, error) {
	return f.filePath, f.serviceName, f.err
}

// countingLocator is a fakeLocator that counts every Locate, so a test can
// assert the fixer preferred the location the trigger carried over the
// promoted-topology lookup.
type countingLocator struct {
	filePath, serviceName string
	calls                 int
}

func (f *countingLocator) Locate(_ context.Context, _ string) (string, string, error) {
	f.calls++
	return f.filePath, f.serviceName, nil
}

// countingEvidence counts every Fetch and always errors, so a test can assert a
// skip path never reached object storage and a fixer that did read fails loudly
// rather than proceeding on fabricated content.
type countingEvidence struct{ calls int }

func (f *countingEvidence) Fetch(_ context.Context, _ string) (string, error) {
	f.calls++
	return "", fmt.Errorf("evidence reader must not be called on a skip path")
}

// fakeUpstream returns fixed upstream changes, or an error, for
// UpstreamChangeReader.UpstreamChanges. calls counts every invocation so a
// test can assert a skip path never queries it.
type fakeUpstream struct {
	changes []prompt.UpstreamChange
	err     error
	calls   int
}

func (f *fakeUpstream) UpstreamChanges(_ context.Context, _ string) ([]prompt.UpstreamChange, error) {
	f.calls++
	return f.changes, f.err
}

// fakeVersions returns a fixed current version, or an error, for
// VersionReader.CurrentVersion. calls counts every invocation so a test can
// assert a skip path never queries it.
type fakeVersions struct {
	v     ports.CurrentVersion
	ok    bool
	err   error
	calls int
}

func (f *fakeVersions) CurrentVersion(_ context.Context, _ string) (ports.CurrentVersion, bool, error) {
	f.calls++
	return f.v, f.ok, f.err
}

// fakeCandidateSource returns a fixed bundle source, or an error, for
// CandidateSourceReader.NodeSource. calls counts every invocation so a test
// can assert a skip path never queries it.
type fakeCandidateSource struct {
	src   ports.CandidateSource
	err   error
	calls int
}

func (f *fakeCandidateSource) NodeSource(_ context.Context, _, _, _ string) (ports.CandidateSource, error) {
	f.calls++
	return f.src, f.err
}

// fakeArtifacts records writes in memory and returns deterministic URIs.
type fakeArtifacts struct {
	written map[string]string
}

func (f *fakeArtifacts) Write(_ context.Context, key, body, _ string) (string, error) {
	if f.written == nil {
		f.written = map[string]string{}
	}
	f.written[key] = body
	return "s3://bucket/" + key, nil
}

// fakeSource returns a fixed file content or an error for ReadFile. readPath
// records the last ReadFile path so tests can assert selection.
type fakeSource struct {
	content  string
	err      error
	readPath string
}

func (f *fakeSource) ReadFile(_ context.Context, _, _, path string) (string, error) {
	f.readPath = path
	return f.content, f.err
}

func (f *fakeSource) ListDir(_ context.Context, _, _, _ string) ([]string, error) {
	return nil, nil
}

// TestValidation_NoCandidateSQL_Skips verifies that a validation trigger with
// no candidate SQL is recorded as skipped and no evidence/LLM/artifact
// collaborator is touched.
func TestValidation_NoCandidateSQL_Skips(t *testing.T) {
	r, err := validationFixer{}.Propose(context.Background(), Services{Logger: testLogger()}, Input{Source: "validation"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

// TestValidation_Step1Empty_Fails verifies that an empty Step-1 LLM result is
// recorded as failed without any artifact writes.
func TestValidation_Step1Empty_Fails(t *testing.T) {
	svc := validationSvc()
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{ProposedSQL: ""}}}

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusFailed {
		t.Fatalf("status = %v want failed", r.Proposal.Status)
	}
}

// validationSvc builds a Services wired for a validation-fix happy path: a
// located source file, a bundle that resolves the candidate source, a current
// version, no upstream changes, and no precedents. Individual tests override
// whichever collaborator their scenario needs.
func validationSvc() Services {
	return Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{
			{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"}, // step 1
			{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},    // step 2
		}},
		Evidence:         fakeEvidence{data: map[string]string{"s3://cand": "SELECT 0", "s3://log": "boom"}},
		Source:           &fakeSource{content: "SELECT 0 -- github"},
		Sanitizer:        fakeSanitizer{},
		Locator:          fakeLocator{filePath: "models/self.sql", serviceName: "svc"},
		CandidateSource:  &fakeCandidateSource{src: ports.CandidateSource{RawCode: "SELECT 0 -- bundle", Runtime: "dbt"}},
		Upstream:         &fakeUpstream{},
		Versions:         &fakeVersions{},
		Precedents:       &fakePrecedents{},
		Artifacts:        &fakeArtifacts{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
}

func validationInput() Input {
	return Input{Source: "validation", ReleaseID: "r", NodeID: "n", Repo: "o/repo", CommitSHA: "sha",
		CandidateArtifactURI: "s3://cand", DBTLogURI: "s3://log",
		CodeBundleURI: "s3://continuo/code-bundles/rel-1/bundle.json", Attempt: 1}
}

// twoStepLLM returns a fresh fake LLM whose Step-1 and Step-2 calls both
// succeed, for tests that only care about the evidence assembled, not the
// LLM's answers.
func twoStepLLM() *fakeLLM {
	return &fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
		{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},
	}}
}

// TestValidation_SourceFromBundle_NoGitHubRead verifies that when the code
// bundle resolves the candidate source, Step 2 never reads GitHub: the
// GitHub fake is set to error if called, and the proposal still resolves the
// source (from the bundle) with FilePath built from the located path.
func TestValidation_SourceFromBundle_NoGitHubRead(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{RawCode: "SELECT 0 -- bundle", Runtime: "dbt"}}
	src := &fakeSource{err: fmt.Errorf("must not be called")}
	svc.Source = src

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v", r.Proposal.Status, r.Proposal.SourceResolved)
	}
	if r.Proposal.FilePath != "services/svc/models/self.sql" {
		t.Fatalf("FilePath = %q, want the located path joined with the repo prefix", r.Proposal.FilePath)
	}
	if src.readPath != "" {
		t.Fatalf("Step 2 must not read GitHub when the bundle resolves the source, read %q", src.readPath)
	}
	require.Len(t, r.Proposal.Edits, 1)
	require.Equal(t, r.Proposal.FilePath, r.Proposal.Edits[0].Path)
	require.Equal(t, r.Proposal.ProposedSQLURI, r.Proposal.Edits[0].ContentURI)
	require.Equal(t, r.Proposal.DiffURI, r.Proposal.Edits[0].DiffURI)
}

// TestValidation_BundleNotFound_FallsBackToGitHub verifies that a bundle miss
// (ports.ErrNotFound) falls back to a GitHub read at the located path, still
// resolving the source.
func TestValidation_BundleNotFound_FallsBackToGitHub(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Source = &fakeSource{content: "SELECT 0 -- github"}

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v, want the github fallback to resolve the source", r.Proposal.Status, r.Proposal.SourceResolved)
	}
}

// TestValidation_BundleEmptyRawCode_FallsBackToGitHub verifies that a bundle
// entry that exists (Runtime dbt, no error) but carries an empty RawCode is
// treated as a permanent miss rather than a resolved-but-empty source: Step 2
// falls back to the GitHub read and resolves the source from there, instead of
// returning early with nothing to diff or fix.
func TestValidation_BundleEmptyRawCode_FallsBackToGitHub(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{RawCode: "", Runtime: "dbt"}}
	svc.Source = &fakeSource{content: "SELECT 0 -- github"}

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v, want the github fallback to resolve the source when the bundle's raw_code is empty",
			r.Proposal.Status, r.Proposal.SourceResolved)
	}
}

// TestValidation_BundleAndGitHubUnavailable_CandidateOnly verifies that when
// both the bundle and the GitHub fallback miss, the proposal degrades to the
// pre-existing candidate-only end state: still proposed, SourceResolved=false.
func TestValidation_BundleAndGitHubUnavailable_CandidateOnly(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Source = &fakeSource{err: ports.ErrSourceNotFound}

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed", r.Proposal.Status)
	}
	if r.Proposal.SourceResolved {
		t.Fatal("expected SourceResolved=false when both the bundle and github are unavailable")
	}
	require.Empty(t, r.Proposal.Edits, "a candidate-only proposal has no resolved repository file path to attach an edit to")
}

// TestValidation_BundleResolvedButLocationUnavailable_CandidateOnly verifies
// that a bundle-resolved candidate source alone never produces a
// source-resolved proposal: the bundle needs no file path to resolve (it is
// keyed by node id), but Step 2 still requires a located file path and a
// mapped service to build the path a PR would target, so it must degrade to
// the candidate-only proposal rather than record one built from an empty or
// unmapped location.
func TestValidation_BundleResolvedButLocationUnavailable_CandidateOnly(t *testing.T) {
	t.Run("locator_error", func(t *testing.T) {
		svc := validationSvc()
		svc.Locator = fakeLocator{err: fmt.Errorf("node location unavailable")}

		r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
		if err != nil {
			t.Fatal(err)
		}
		if r.Proposal.Status != proposal.StatusProposed {
			t.Fatalf("status = %v want proposed", r.Proposal.Status)
		}
		if r.Proposal.SourceResolved {
			t.Fatal("expected SourceResolved=false when the node cannot be located, even though the bundle resolved a candidate source")
		}
		require.Empty(t, r.Proposal.Edits, "a candidate-only proposal has no resolved repository file path to attach an edit to")
	})

	t.Run("unmapped_service", func(t *testing.T) {
		svc := validationSvc()
		svc.Locator = fakeLocator{filePath: "models/self.sql", serviceName: "unknown-service"}

		r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
		if err != nil {
			t.Fatal(err)
		}
		if r.Proposal.Status != proposal.StatusProposed {
			t.Fatalf("status = %v want proposed", r.Proposal.Status)
		}
		if r.Proposal.SourceResolved {
			t.Fatal("expected SourceResolved=false when the located service has no repo mapping, even though the bundle resolved a candidate source")
		}
		require.Empty(t, r.Proposal.Edits, "a candidate-only proposal has no resolved repository file path to attach an edit to")
	})
}

// TestValidation_BundleTransientError_Redelivers verifies that a transient
// bundle fetch error (not ports.ErrNotFound) propagates as an error rather
// than degrading, so the trigger is redelivered and no proposal is produced.
func TestValidation_BundleTransientError_Redelivers(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: fmt.Errorf("s3 5xx")}

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err == nil {
		t.Fatal("expected a transient bundle error to be returned so the trigger redelivers")
	}
	if r.Proposal.Status != "" {
		t.Fatalf("expected no proposal on a transient bundle error, got %+v", r.Proposal)
	}
}

// TestValidation_OwnChangeDiff_InPrompt verifies that when the node has a
// recorded current version, the unified diff from that version to the
// resolved candidate source is embedded in the Step-1 prompt under "What this
// release changed".
func TestValidation_OwnChangeDiff_InPrompt(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{RawCode: "select bad", Runtime: "dbt"}}
	svc.Versions = &fakeVersions{v: ports.CurrentVersion{RawCode: "select good"}, ok: true}
	llm := twoStepLLM()
	svc.LLM = llm

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if len(llm.requests) == 0 || !strings.Contains(llm.requests[0].User, "What this release changed") {
		t.Fatalf("step-1 prompt must contain the own-change diff section:\n%s", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, "-select good") || !strings.Contains(llm.requests[0].User, "+select bad") {
		t.Fatalf("step-1 prompt must contain the diff of the last promoted version to the candidate:\n%s", llm.requests[0].User)
	}
}

// TestValidation_OwnChangeDiff_Sanitized verifies the own-change diff is run
// through the LogSanitizer before it is embedded in the Step-1 prompt, so a
// secret in a changed line is redacted rather than sent to the external LLM.
func TestValidation_OwnChangeDiff_Sanitized(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{RawCode: "select 1 -- password = SEKRET", Runtime: "dbt"}}
	svc.Versions = &fakeVersions{v: ports.CurrentVersion{RawCode: "select 1"}, ok: true}
	svc.Sanitizer = redactingSanitizer{secret: "SEKRET", marker: "[redacted]"}
	llm := twoStepLLM()
	svc.LLM = llm

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(llm.requests[0].User, "SEKRET") {
		t.Fatalf("unsanitized secret leaked into the prompt via the own-change diff:\n%s", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, "[redacted]") {
		t.Fatalf("sanitized marker missing from the embedded own-change diff:\n%s", llm.requests[0].User)
	}
}

// TestValidation_NoCurrentVersion_NoOwnChangeSection verifies that when the
// node has no recorded current version, the prompt has no own-change section,
// and the proposal is still produced.
func TestValidation_NoCurrentVersion_NoOwnChangeSection(t *testing.T) {
	svc := validationSvc()
	svc.Versions = &fakeVersions{ok: false}
	llm := twoStepLLM()
	svc.LLM = llm

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed", r.Proposal.Status)
	}
	if strings.Contains(llm.requests[0].User, "What this release changed") {
		t.Fatalf("no current version means no own-change section:\n%s", llm.requests[0].User)
	}
}

// TestValidation_UpstreamChanges_InPrompt verifies that an upstream change's
// code diff and resolved-config diff both render in the Step-1 prompt.
func TestValidation_UpstreamChanges_InPrompt(t *testing.T) {
	svc := validationSvc()
	svc.Upstream = &fakeUpstream{changes: []prompt.UpstreamChange{
		{NodeID: "analytics.payments", Depth: 1, CodeDiff: "-a\n+b",
			ConfigDiff: `-"materialized": "table"` + "\n" + `+"materialized": "incremental"`},
	}}
	llm := twoStepLLM()
	svc.LLM = llm

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(llm.requests[0].User, "analytics.payments") || !strings.Contains(llm.requests[0].User, "-a\n+b") {
		t.Fatalf("step-1 prompt missing the upstream code diff:\n%s", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, `+"materialized": "incremental"`) {
		t.Fatalf("step-1 prompt missing the upstream config diff:\n%s", llm.requests[0].User)
	}
}

// TestValidation_UpstreamChanges_Sanitized verifies that an upstream change's
// code diff and config diff are both run through the LogSanitizer before they
// are embedded in the Step-1 prompt, so a secret in either is redacted rather
// than sent to the external LLM.
func TestValidation_UpstreamChanges_Sanitized(t *testing.T) {
	svc := validationSvc()
	svc.Upstream = &fakeUpstream{changes: []prompt.UpstreamChange{
		{NodeID: "analytics.payments", Depth: 1,
			CodeDiff:   "-a\n+password = SEKRET",
			ConfigDiff: `-"x": "y"` + "\n" + `+"token": "SEKRET"`},
	}}
	svc.Sanitizer = redactingSanitizer{secret: "SEKRET", marker: "[redacted]"}
	llm := twoStepLLM()
	svc.LLM = llm

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(llm.requests[0].User, "SEKRET") {
		t.Fatalf("unsanitized secret leaked into the prompt via an upstream diff:\n%s", llm.requests[0].User)
	}
	if strings.Count(llm.requests[0].User, "[redacted]") != 2 {
		t.Fatalf("expected the marker in place of both the code and config diff secrets:\n%s", llm.requests[0].User)
	}
}

// TestValidation_PrecedentFields_Sanitized verifies that a precedent's
// resolution diff and error excerpt are both run through the LogSanitizer
// before they are embedded in the Step-1 prompt's "How similar failures were
// fixed before" section, so a secret in either is redacted rather than sent
// to the external LLM.
func TestValidation_PrecedentFields_Sanitized(t *testing.T) {
	svc := validationSvc()
	svc.Precedents = &fakePrecedents{bySignature: map[string][]prompt.Precedent{
		"sig-1": {
			{
				ReleaseID: "r-other", NodeID: "other-node",
				Category: "validation", Reason: "type_mismatch",
				ErrorExcerpt:   "column x default password = SEKRET",
				RejectedAt:     "2026-01-01T00:00:00Z",
				Resolved:       true,
				ResolutionDiff: "-x\n+password = SEKRET",
			},
		},
	}}
	svc.Sanitizer = redactingSanitizer{secret: "SEKRET", marker: "[redacted]"}
	llm := twoStepLLM()
	svc.LLM = llm

	in := validationInput()
	in.ErrorSignature = "sig-1"

	if _, err := (validationFixer{}).Propose(context.Background(), svc, in); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(llm.requests[0].User, "SEKRET") {
		t.Fatalf("unsanitized secret leaked into the prompt via a precedent:\n%s", llm.requests[0].User)
	}
	if strings.Count(llm.requests[0].User, "[redacted]") != 2 {
		t.Fatalf("expected the marker in place of both the precedent's error excerpt and resolution diff:\n%s", llm.requests[0].User)
	}
}

// TestValidation_PrecedentEditedDiff_Sanitized verifies that a precedent's
// Edited entries — the diffs of the nodes a merged fix PR touched — are run
// through the LogSanitizer before they reach the LLM, the same as the
// precedent's own resolution diff and error excerpt.
func TestValidation_PrecedentEditedDiff_Sanitized(t *testing.T) {
	svc := validationSvc()
	svc.Precedents = &fakePrecedents{bySignature: map[string][]prompt.Precedent{
		"sig-1": {
			{
				ReleaseID: "r-other", NodeID: "other-node",
				Category: "validation", Reason: "type_mismatch",
				ErrorExcerpt: "column x does not exist",
				RejectedAt:   "2026-01-01T00:00:00Z",
				Resolved:     true,
				Edited: []prompt.EditedPrecedent{
					{NodeID: "upstream-node", Path: "models/upstream.sql", Diff: "-x\n+password = SEKRET"},
				},
			},
		},
	}}
	svc.Sanitizer = redactingSanitizer{secret: "SEKRET", marker: "[redacted]"}
	llm := twoStepLLM()
	svc.LLM = llm

	in := validationInput()
	in.ErrorSignature = "sig-1"

	if _, err := (validationFixer{}).Propose(context.Background(), svc, in); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(llm.requests[0].User, "SEKRET") {
		t.Fatalf("unsanitized secret leaked into the prompt via a precedent's edited diff:\n%s", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, "[redacted]") {
		t.Fatalf("expected the marker in place of the edited diff's secret:\n%s", llm.requests[0].User)
	}
}

// TestValidation_GraphReadsFail_DegradesToBaseEvidence verifies that when the
// upstream, version, and precedent lookups all error, the fixer still
// proposes a fix from the candidate SQL and dbt log alone.
func TestValidation_GraphReadsFail_DegradesToBaseEvidence(t *testing.T) {
	svc := validationSvc()
	svc.Upstream = &fakeUpstream{err: fmt.Errorf("graph unavailable")}
	svc.Versions = &fakeVersions{err: fmt.Errorf("graph unavailable")}
	svc.Precedents = &fakePrecedents{err: fmt.Errorf("neo4j unavailable")}

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed even when every graph read degrades", r.Proposal.Status)
	}
}

// TestValidation_EmptyCandidateArtifactURI_SkipsBeforeAnyRead verifies that a
// validation trigger with no candidate artifact is skipped before the
// candidate-source, upstream, version, or precedent lookups are ever consulted,
// so a transiently unreadable collaborator cannot turn the intended skip into a
// redelivery. This covers a rejection with nothing to fix — a dbt seed, whose
// candidate_artifact_uri is empty. It is NOT what protects the python path: a
// python node's rejection carries a non-empty candidate_artifact_uri (a JSON
// validation spec), which is guarded by node_type instead — see
// TestValidation_PythonNode_SkipsBeforeAnyRead.
func TestValidation_EmptyCandidateArtifactURI_SkipsBeforeAnyRead(t *testing.T) {
	cs := &fakeCandidateSource{err: fmt.Errorf("must not be called")}
	up := &fakeUpstream{err: fmt.Errorf("must not be called")}
	vs := &fakeVersions{err: fmt.Errorf("must not be called")}
	pr := &fakePrecedents{err: fmt.Errorf("must not be called")}
	llm := &fakeLLM{}
	svc := Services{
		LLM: llm, CandidateSource: cs, Upstream: up, Versions: vs, Precedents: pr,
		Logger: testLogger(),
	}
	in := Input{Source: "validation", ReleaseID: "r", NodeID: "n", CandidateArtifactURI: ""}

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if llm.calls != 0 {
		t.Fatalf("expected no LLM call with no candidate artifact, got %d", llm.calls)
	}
	if cs.calls != 0 || up.calls != 0 || vs.calls != 0 || pr.calls != 0 {
		t.Fatalf("expected zero calls to the candidate-source/upstream/versions/precedent fakes, got cs=%d up=%d vs=%d pr=%d",
			cs.calls, up.calls, vs.calls, pr.calls)
	}
}

// TestValidation_PythonNode_SkipsBeforeAnyRead enforces the non-negotiable
// project invariant that no remediation path ever produces an LLM call or a
// proposal for a python node, on the shape a python node actually produces:
// topology-controller uploads a JSON validation spec for a python node, so its
// validation rejection carries a NON-EMPTY candidate_artifact_uri and the
// empty-URI skip never fires. node_type on the trigger is what guards it, and
// it must fire before the candidate artifact, the dbt log, the node location,
// the code bundle, the current version, upstream changes, or precedent are
// read.
func TestValidation_PythonNode_SkipsBeforeAnyRead(t *testing.T) {
	ev := &countingEvidence{}
	cs := &fakeCandidateSource{src: ports.CandidateSource{
		RawCode: `{"reads":["analytics.orders"],"output_columns":[]}`, Runtime: "python"}}
	loc := &countingLocator{filePath: "python/report.py", serviceName: "svc"}
	up := &fakeUpstream{}
	vs := &fakeVersions{}
	pr := &fakePrecedents{}
	llm := &fakeLLM{}
	svc := Services{
		LLM: llm, Evidence: ev, CandidateSource: cs, Locator: loc,
		Upstream: up, Versions: vs, Precedents: pr, Logger: testLogger(),
	}
	in := validationInput()
	in.NodeType = "python-model"
	in.FilePath, in.Service = "python/report.py", "svc"
	in.CandidateArtifactURI = "s3://continuo/candidate-sql/r/candidate_n.json"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if r.Proposal.SourceResolved {
		t.Fatal("a python node must never be recorded as source-resolved")
	}
	if llm.calls != 0 {
		t.Fatalf("expected no LLM call for a python node, got %d", llm.calls)
	}
	if ev.calls != 0 {
		t.Fatalf("expected the evidence reader to be untouched, got %d calls", ev.calls)
	}
	if cs.calls != 0 || loc.calls != 0 || up.calls != 0 || vs.calls != 0 || pr.calls != 0 {
		t.Fatalf("expected zero calls to the candidate-source/locator/upstream/versions/precedent fakes, got cs=%d loc=%d up=%d vs=%d pr=%d",
			cs.calls, loc.calls, up.calls, vs.calls, pr.calls)
	}
}

// TestValidation_PythonCsvNode_SkipsBeforeAnyRead mirrors
// TestValidation_PythonNode_SkipsBeforeAnyRead for the python-csv node kind: a
// csv node is part of the python family (IsPython), so it must be skipped by
// this defensive fallback the same way a python-model node is.
func TestValidation_PythonCsvNode_SkipsBeforeAnyRead(t *testing.T) {
	ev := &countingEvidence{}
	cs := &fakeCandidateSource{src: ports.CandidateSource{
		RawCode: `{"reads":["analytics.orders"],"output_columns":[]}`, Runtime: "python"}}
	loc := &countingLocator{filePath: "csv/report.csv", serviceName: "svc"}
	up := &fakeUpstream{}
	vs := &fakeVersions{}
	pr := &fakePrecedents{}
	llm := &fakeLLM{}
	svc := Services{
		LLM: llm, Evidence: ev, CandidateSource: cs, Locator: loc,
		Upstream: up, Versions: vs, Precedents: pr, Logger: testLogger(),
	}
	in := validationInput()
	in.NodeType = "python-csv"
	in.FilePath, in.Service = "csv/report.csv", "svc"
	in.CandidateArtifactURI = "s3://continuo/candidate-sql/r/candidate_n.json"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if r.Proposal.SourceResolved {
		t.Fatal("a python-csv node must never be recorded as source-resolved")
	}
	if llm.calls != 0 {
		t.Fatalf("expected no LLM call for a python-csv node, got %d", llm.calls)
	}
	if ev.calls != 0 {
		t.Fatalf("expected the evidence reader to be untouched, got %d calls", ev.calls)
	}
	if cs.calls != 0 || loc.calls != 0 || up.calls != 0 || vs.calls != 0 || pr.calls != 0 {
		t.Fatalf("expected zero calls to the candidate-source/locator/upstream/versions/precedent fakes, got cs=%d loc=%d up=%d vs=%d pr=%d",
			cs.calls, loc.calls, up.calls, vs.calls, pr.calls)
	}
}

// TestValidation_BundleRuntimeNonDbt_NoProposal covers a trigger that carries no
// node_type — the shape emitted before a validation rejection named the failing
// node's kind. The only remaining signal is the runtime the code bundle records
// for the node, and a non-dbt bundle entry's raw_code is the node's normalized
// contract entry rather than model source. The fixer must skip on it: no LLM
// call, and no repo read substituting for the bundle entry either.
func TestValidation_BundleRuntimeNonDbt_NoProposal(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{
		RawCode: `{"reads":["analytics.orders"],"output_columns":[]}`, Runtime: "python"}}
	src := &fakeSource{content: "print('hello')"}
	svc.Source = src
	llm := twoStepLLM()
	svc.LLM = llm

	in := validationInput() // no node_type, as an older trigger carries none
	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if llm.calls != 0 {
		t.Fatalf("expected no LLM call for a non-dbt bundle entry, got %d", llm.calls)
	}
	if src.readPath != "" {
		t.Fatalf("a repo read must not substitute for a non-dbt bundle entry; read %q", src.readPath)
	}
}

// TestValidation_BundleRuntimeNonDbt_TrustedNodeType_StillSkips verifies that
// the bundle-runtime check fires regardless of node_type: a trigger carrying
// a trusted node_type (dbt-model) must not override the bundle's own recorded
// runtime. Unlike the fallback path's extension check, which is trusted away
// once node_type is present, the bundle's recorded runtime is authoritative
// whenever the bundle resolves at all — the node_type guard is decided before
// the bundle is ever read and cannot see what the bundle later says.
func TestValidation_BundleRuntimeNonDbt_TrustedNodeType_StillSkips(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{
		RawCode: `{"reads":["analytics.orders"],"output_columns":[]}`, Runtime: "python"}}
	src := &fakeSource{content: "print('hello')"}
	svc.Source = src
	llm := twoStepLLM()
	svc.LLM = llm

	in := validationInput()
	in.NodeType = "dbt-model" // trusted node_type must not override the bundle's recorded runtime
	in.FilePath, in.Service = "models/self.sql", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if llm.calls != 0 {
		t.Fatalf("expected no LLM call for a non-dbt bundle entry even with a trusted node_type, got %d", llm.calls)
	}
	if src.readPath != "" {
		t.Fatalf("a repo read must not substitute for a non-dbt bundle entry even with a trusted node_type; read %q", src.readPath)
	}
}

// TestValidation_FallbackPathNonSQL_NoProposal covers a trigger that carries no
// node_type (the legacy shape: an older trigger, or an outbox replay) and
// whose code bundle permanently misses: with no node_type to trust, the only
// remaining signal is the resolved fallback path itself, and a path that does
// not end in ".sql" is a python node's script — the fixer must skip on it
// before ever reading it: no LLM call, and the GitHub source reader is never
// consulted. NodeType is set to "" explicitly (not just left at Input's zero
// value) so the test proves this is the no-node_type path, not an accident of
// the fixture's defaults.
func TestValidation_FallbackPathNonSQL_NoProposal(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	src := &fakeSource{content: "print('hello')"}
	svc.Source = src
	llm := twoStepLLM()
	svc.LLM = llm

	in := validationInput()
	in.NodeType = ""
	in.FilePath, in.Service = "python/report.py", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if llm.calls != 0 {
		t.Fatalf("expected no LLM call for a non-sql fallback path, got %d", llm.calls)
	}
	if src.readPath != "" {
		t.Fatalf("the github source reader must not be called for a non-sql fallback path; read %q", src.readPath)
	}
}

// TestValidation_FallbackPathSQL_NoNodeType_StillResolvesFromGitHub is the
// regression guard for TestValidation_FallbackPathNonSQL_NoProposal: a trigger
// with no node_type, a permanent bundle miss, and a ".sql" fallback path falls
// back to the repo read and resolves the source.
func TestValidation_FallbackPathSQL_NoNodeType_StillResolvesFromGitHub(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Source = &fakeSource{content: "SELECT 0 -- github"}

	in := validationInput()
	in.NodeType = ""
	in.FilePath, in.Service = "models/report.sql", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v, want the github fallback to resolve a .sql candidate",
			r.Proposal.Status, r.Proposal.SourceResolved)
	}
}

// TestValidation_ModelFallbackPathSQL_WithNodeType_Resolves verifies that a
// trigger carrying node_type=dbt-model and a ".sql" fallback path resolves the
// source from the repo read on a permanent bundle miss: an explicit dbt
// node_type is trusted, and its path already ends in ".sql".
func TestValidation_ModelFallbackPathSQL_WithNodeType_Resolves(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Source = &fakeSource{content: "SELECT 0 -- github"}

	in := validationInput()
	in.NodeType = "dbt-model"
	in.FilePath, in.Service = "models/report.sql", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v, want a dbt-model's .sql fallback path to resolve from github",
			r.Proposal.Status, r.Proposal.SourceResolved)
	}
}

// TestValidation_SnapshotFallbackPathYAML_Resolves verifies that a trigger
// carrying node_type=dbt-snapshot trusts that node_type over the fallback
// path's extension: a dbt snapshot's source is legitimately a ".yml" file, so
// a permanent bundle miss still falls back to the repo read and resolves the
// source, rather than the ".yml" path being misclassified as non-dbt.
func TestValidation_SnapshotFallbackPathYAML_Resolves(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Source = &fakeSource{content: "target_schema: analytics\nstrategy: timestamp\n"}

	in := validationInput()
	in.NodeType = "dbt-snapshot"
	in.FilePath, in.Service = "snapshots/orders.yml", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v, want a dbt-snapshot's .yml fallback path to resolve from github",
			r.Proposal.Status, r.Proposal.SourceResolved)
	}
}

// TestValidation_TrustedNodeType_UnreadableFallbackPath_DegradesToCandidateOnly
// verifies that a trusted node_type (dbt-snapshot) bypasses the extension
// check but still goes through the actual repo read, and an unreadable path
// there degrades quietly to the candidate-only proposal — StatusProposed with
// SourceResolved=false — the same as any other repo-read failure, and
// specifically not StatusSkipped. Trusting node_type over the extension must
// not also bypass read-failure handling.
func TestValidation_TrustedNodeType_UnreadableFallbackPath_DegradesToCandidateOnly(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{err: ports.ErrNotFound}
	svc.Source = &fakeSource{err: ports.ErrSourceNotFound}

	in := validationInput()
	in.NodeType = "dbt-snapshot"
	in.FilePath, in.Service = "snapshots/orders.yml", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed (candidate-only degrade), not skipped", r.Proposal.Status)
	}
	if r.Proposal.SourceResolved {
		t.Fatal("expected SourceResolved=false when the trusted node_type's fallback path is unreadable")
	}
}

// TestValidation_BundlePrimaryPath_IgnoresFileExtension verifies that a dbt
// bundle entry with non-empty raw_code resolves the candidate source before
// the file-extension guard is ever reached, regardless of what the located
// path looks like: the guard applies to the fallback repo read only.
func TestValidation_BundlePrimaryPath_IgnoresFileExtension(t *testing.T) {
	svc := validationSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{RawCode: "SELECT 0 -- bundle", Runtime: "dbt"}}
	src := &fakeSource{err: fmt.Errorf("must not be called")}
	svc.Source = src

	in := validationInput() // no node_type
	in.FilePath, in.Service = "python/report.py", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v, want the bundle to resolve regardless of the located path's extension",
			r.Proposal.Status, r.Proposal.SourceResolved)
	}
	if src.readPath != "" {
		t.Fatalf("the github source reader must not be called when the bundle resolves the source; read %q", src.readPath)
	}
}

// TestValidation_CandidateLocationPreferredOverPromotedTopology verifies that a
// trigger carrying the candidate topology's own file path and service targets
// that path, and never consults the promoted-topology lookup. The rejected
// release was never promoted, so that lookup would return the PREVIOUS release's
// path for a node whose candidate moved it — writing the fix to a file the
// candidate no longer has.
func TestValidation_CandidateLocationPreferredOverPromotedTopology(t *testing.T) {
	svc := validationSvc()
	loc := &countingLocator{filePath: "models/old_home.sql", serviceName: "svc"}
	svc.Locator = loc

	in := validationInput()
	in.FilePath, in.Service = "models/staging/moved.sql", "svc"

	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v", r.Proposal.Status, r.Proposal.SourceResolved)
	}
	if r.Proposal.FilePath != "services/svc/models/staging/moved.sql" {
		t.Fatalf("file path = %q want the candidate's own path", r.Proposal.FilePath)
	}
	if loc.calls != 0 {
		t.Fatalf("expected the promoted-topology lookup to be skipped, got %d calls", loc.calls)
	}
}

// TestValidation_NoCandidateLocation_FallsBackToPromotedTopology verifies that a
// trigger carrying no location still targets the path the promoted topology
// reports, so a rejection emitted before the candidate location was carried
// keeps resolving its source.
func TestValidation_NoCandidateLocation_FallsBackToPromotedTopology(t *testing.T) {
	svc := validationSvc()
	loc := &countingLocator{filePath: "models/self.sql", serviceName: "svc"}
	svc.Locator = loc

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.FilePath != "services/svc/models/self.sql" {
		t.Fatalf("file path = %q want the promoted-topology path", r.Proposal.FilePath)
	}
	if loc.calls != 1 {
		t.Fatalf("expected exactly one promoted-topology lookup, got %d", loc.calls)
	}
}

// TestTruncateDiff_ShortStringUnchanged verifies a diff at or under the cap is
// returned verbatim, with no truncation marker.
func TestTruncateDiff_ShortStringUnchanged(t *testing.T) {
	s := "a short diff"
	if got := truncateDiff(s, 100); got != s {
		t.Fatalf("got %q, want the input unchanged", got)
	}
}

// TestTruncateDiff_BacksUpOffMultibyteRune verifies the cut is moved to a rune
// boundary so a multibyte character is never split: the result must be valid
// UTF-8 and must drop the partial rune rather than emit a replacement character.
func TestTruncateDiff_BacksUpOffMultibyteRune(t *testing.T) {
	// "é" is 2 bytes (0xC3 0xA9). Byte index 6 lands on the continuation byte of
	// the first "é", so the cut must back up to index 5 (end of the ASCII run).
	s := strings.Repeat("a", 5) + strings.Repeat("é", 5) // 5 + 10 bytes
	got := truncateDiff(s, 6)

	if !utf8.ValidString(got) {
		t.Fatalf("result is not valid UTF-8: %q", got)
	}
	kept := strings.TrimSuffix(got, "\n… (diff truncated)")
	if kept != "aaaaa" {
		t.Fatalf("kept = %q, want %q (cut backed up off the split rune)", kept, "aaaaa")
	}
	if !strings.Contains(got, "diff truncated") {
		t.Fatalf("missing truncation marker: %q", got)
	}
}
