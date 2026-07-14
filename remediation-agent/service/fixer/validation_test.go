package fixer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
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

// fakeAncestry returns a fixed file path, service name, and ancestor slice,
// or an error if set.
type fakeAncestry struct {
	filePath string
	service  string
	ancestry []prompt.Ancestor
	err      error
}

func (f fakeAncestry) NodeContext(_ context.Context, _ string) (string, string, []prompt.Ancestor, error) {
	return f.filePath, f.service, f.ancestry, f.err
}

// fakeSanitizer is a pass-through log sanitizer.
type fakeSanitizer struct{}

func (fakeSanitizer) Sanitize(s string) string { return s }

// fakeLLM returns results from a queue (one per Propose call, in order). When
// the queue is exhausted, the last entry is repeated.
type fakeLLM struct {
	queue    []ports.ProposeResult
	errs     []error
	calls    int
	requests []ports.ProposeRequest // records each request in call order
}

func (f *fakeLLM) Propose(_ context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	f.requests = append(f.requests, req)
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

// fakeSource returns a fixed file content or an error for ReadFile, and a fixed
// diff or an error for CommitFileDiff. readPath records the last ReadFile path;
// diffPaths records every CommitFileDiff path so tests can assert selection.
type fakeSource struct {
	content   string
	err       error
	readPath  string
	diff      string
	diffErr   error
	diffPaths []string
}

func (f *fakeSource) ReadFile(_ context.Context, _, _, path string) (string, error) {
	f.readPath = path
	return f.content, f.err
}

func (f *fakeSource) ListDir(_ context.Context, _, _, _ string) ([]string, error) {
	return nil, nil
}

func (f *fakeSource) CommitFileDiff(_ context.Context, _, _, path string) (string, error) {
	f.diffPaths = append(f.diffPaths, path)
	return f.diff, f.diffErr
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

// TestValidation_Step2Success_ResolvesSource verifies that when Ancestry, the
// service repo mapping, and the source read all succeed, and Step-2 returns a
// confident, changed result, the final proposal points to the source
// artifacts, SourceResolved is true, FilePath is the joined repo path, and the
// candidate artifacts (Step 1) are still present on the proposal.
func TestValidation_Step2Success_ResolvesSource(t *testing.T) {
	svc := Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{
			{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"}, // step 1
			{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},    // step 2
		}},
		Evidence:         fakeEvidence{data: map[string]string{"s3://cand": "SELECT 0", "s3://log": "boom"}},
		Source:           &fakeSource{content: "SELECT 0 -- original"},
		Sanitizer:        fakeSanitizer{},
		Ancestry:         fakeAncestry{filePath: "models/x.sql", service: "svc"},
		Artifacts:        &fakeArtifacts{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "validation", ReleaseID: "r", NodeID: "n", Repo: "o/repo", CommitSHA: "sha",
		CandidateSQLURI: "s3://cand", DBTLogURI: "s3://log", Attempt: 1}
	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || !r.Proposal.SourceResolved {
		t.Fatalf("got status=%v sourceResolved=%v", r.Proposal.Status, r.Proposal.SourceResolved)
	}
	if r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("FilePath = %q", r.Proposal.FilePath)
	}
	if r.Proposal.Repo != "o/repo" || r.Proposal.CommitSHA != "sha" {
		t.Fatalf("Repo/CommitSHA not set on source-resolved proposal: %+v", r.Proposal)
	}
	if r.Proposal.CandidateFixSQLURI == "" || r.Proposal.CandidateFixDiffURI == "" {
		t.Fatal("candidate artifacts must be written unconditionally")
	}
	if r.Proposal.ProposedSQLURI == r.Proposal.CandidateFixSQLURI {
		t.Fatal("final ProposedSQLURI must point to the source artifact, not the candidate")
	}
}

// TestValidation_Step2Degrade_SourceReadError verifies that when the source
// read fails, the handler falls back to the candidate proposal:
// SourceResolved=false, FilePath/Repo/CommitSHA empty, candidate artifacts
// still written, status still proposed (step 1 succeeded).
func TestValidation_Step2Degrade_SourceReadError(t *testing.T) {
	svc := Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{
			{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
		}},
		Evidence:         fakeEvidence{data: map[string]string{"s3://cand": "SELECT 0", "s3://log": "boom"}},
		Source:           &fakeSource{err: fmt.Errorf("github 503")},
		Sanitizer:        fakeSanitizer{},
		Ancestry:         fakeAncestry{filePath: "models/x.sql", service: "svc"},
		Artifacts:        &fakeArtifacts{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "validation", ReleaseID: "r", NodeID: "n", Repo: "o/repo", CommitSHA: "sha",
		CandidateSQLURI: "s3://cand", DBTLogURI: "s3://log", Attempt: 1}
	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed", r.Proposal.Status)
	}
	if r.Proposal.SourceResolved {
		t.Fatal("expected SourceResolved=false on source read error")
	}
	if r.Proposal.FilePath != "" || r.Proposal.Repo != "" || r.Proposal.CommitSHA != "" {
		t.Fatalf("expected empty source-location fields on degrade, got %+v", r.Proposal)
	}
	if r.Proposal.CandidateFixSQLURI == "" || r.Proposal.CandidateFixDiffURI == "" {
		t.Fatal("candidate artifacts must still be written on degrade")
	}
	if r.Proposal.ProposedSQLURI != r.Proposal.CandidateFixSQLURI {
		t.Fatal("final ProposedSQLURI must fall back to the candidate on degrade")
	}
}

// TestValidation_Step1Empty_Fails verifies that an empty Step-1 LLM result is
// recorded as failed without any artifact writes.
func TestValidation_Step1Empty_Fails(t *testing.T) {
	svc := Services{
		LLM:              &fakeLLM{queue: []ports.ProposeResult{{ProposedSQL: ""}}},
		Evidence:         fakeEvidence{data: map[string]string{"s3://cand": "SELECT 0", "s3://log": "boom"}},
		Source:           &fakeSource{content: "SELECT 0 -- original"},
		Sanitizer:        fakeSanitizer{},
		Ancestry:         fakeAncestry{filePath: "models/x.sql", service: "svc"},
		Artifacts:        &fakeArtifacts{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "validation", ReleaseID: "r", NodeID: "n", Repo: "o/repo", CommitSHA: "sha",
		CandidateSQLURI: "s3://cand", DBTLogURI: "s3://log", Attempt: 1}
	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusFailed {
		t.Fatalf("status = %v want failed", r.Proposal.Status)
	}
}

// TestValidation_AncestryError_ProceedsDegraded verifies the best-effort
// Ancestry contract: an Ancestry error must not fail the fixer. It proceeds
// with nil ancestors and empty filePath/serviceName, which in turn means Step 2
// degrades to the candidate proposal (no repo mapping is possible without a
// service name).
func TestValidation_AncestryError_ProceedsDegraded(t *testing.T) {
	svc := Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{
			{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
		}},
		Evidence:         fakeEvidence{data: map[string]string{"s3://cand": "SELECT 0", "s3://log": "boom"}},
		Source:           &fakeSource{err: fmt.Errorf("must not be called")},
		Sanitizer:        fakeSanitizer{},
		Ancestry:         fakeAncestry{err: fmt.Errorf("ancestry unavailable")},
		Artifacts:        &fakeArtifacts{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "validation", ReleaseID: "r", NodeID: "n", Repo: "o/repo", CommitSHA: "sha",
		CandidateSQLURI: "s3://cand", DBTLogURI: "s3://log", Attempt: 1}
	r, err := validationFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed", r.Proposal.Status)
	}
	if r.Proposal.SourceResolved {
		t.Fatal("expected SourceResolved=false when ancestry errors")
	}
}

// validationSvc builds a Services wired for a validation-fix happy path with the
// given ancestors and source.
func validationSvc(ancestors []prompt.Ancestor, src *fakeSource) Services {
	return Services{
		LLM: &fakeLLM{queue: []ports.ProposeResult{
			{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
			{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},
		}},
		Evidence:         fakeEvidence{data: map[string]string{"s3://cand": "SELECT 0", "s3://log": "boom"}},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Ancestry:         fakeAncestry{filePath: "models/self.sql", service: "svc", ancestry: ancestors},
		Artifacts:        &fakeArtifacts{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc", "up-svc": "services/up-svc"},
	}
}

func validationInput() Input {
	return Input{Source: "validation", ReleaseID: "r", NodeID: "n", Repo: "o/repo", CommitSHA: "sha",
		CandidateSQLURI: "s3://cand", DBTLogURI: "s3://log", Attempt: 1}
}

// TestValidation_UpstreamDiffs_EmbeddedInStep1Prompt verifies that a changed
// ancestor's diff is fetched at its own repo/commit and rendered into the Step-1
// prompt.
func TestValidation_UpstreamDiffs_EmbeddedInStep1Prompt(t *testing.T) {
	llm := &fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
		{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},
	}}
	src := &fakeSource{content: "SELECT 0 -- original", diff: "@@ -1 +1 @@\n-old_col\n+new_col"}
	svc := validationSvc([]prompt.Ancestor{
		{NodeID: "up.node", ServiceName: "up-svc", LastCommitSHA: "upsha", LastRepo: "o/up-repo", FilePath: "models/up.sql", Depth: 1},
	}, src)
	svc.LLM = llm

	_, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if len(src.diffPaths) != 1 || src.diffPaths[0] != "services/up-svc/models/up.sql" {
		t.Fatalf("CommitFileDiff paths = %v, want [services/up-svc/models/up.sql]", src.diffPaths)
	}
	if len(llm.requests) == 0 || !strings.Contains(llm.requests[0].User, "new_col") {
		t.Fatalf("step-1 prompt must contain the upstream diff:\n%s", llm.requests[0].User)
	}
}

// TestValidation_UpstreamDiffs_SkipsUnstampedAncestors verifies ancestors without
// a commit sha, repo, file path, or a known service mapping are not fetched.
func TestValidation_UpstreamDiffs_SkipsUnstampedAncestors(t *testing.T) {
	src := &fakeSource{content: "SELECT 0 -- original", diff: "@@ x @@"}
	svc := validationSvc([]prompt.Ancestor{
		{NodeID: "no-sha", ServiceName: "up-svc", LastRepo: "o/up-repo", FilePath: "models/a.sql", Depth: 1},
		{NodeID: "no-repo", ServiceName: "up-svc", LastCommitSHA: "s", FilePath: "models/b.sql", Depth: 1},
		{NodeID: "no-map", ServiceName: "unknown", LastCommitSHA: "s", LastRepo: "o/x", FilePath: "c.sql", Depth: 1},
	}, src)

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if len(src.diffPaths) != 0 {
		t.Fatalf("no diff should be fetched for unstamped/unmapped ancestors, got %v", src.diffPaths)
	}
}

// TestValidation_UpstreamDiffs_CapsAtFive verifies at most maxUpstreamDiffs diffs
// are fetched even when more eligible ancestors are present.
func TestValidation_UpstreamDiffs_CapsAtFive(t *testing.T) {
	src := &fakeSource{content: "SELECT 0 -- original", diff: "@@ x @@"}
	var ancestors []prompt.Ancestor
	for i := 0; i < 8; i++ {
		ancestors = append(ancestors, prompt.Ancestor{
			NodeID: fmt.Sprintf("up.%d", i), ServiceName: "up-svc", LastCommitSHA: "s",
			LastRepo: "o/up-repo", FilePath: fmt.Sprintf("models/up%d.sql", i), Depth: 1,
		})
	}
	svc := validationSvc(ancestors, src)

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if len(src.diffPaths) != maxUpstreamDiffs {
		t.Fatalf("fetched %d diffs, want cap of %d", len(src.diffPaths), maxUpstreamDiffs)
	}
}

// TestValidation_UpstreamDiffs_BestEffortOnError verifies a CommitFileDiff error
// is swallowed: the proposal is still produced and no diff block is added.
func TestValidation_UpstreamDiffs_BestEffortOnError(t *testing.T) {
	llm := &fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
		{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},
	}}
	src := &fakeSource{content: "SELECT 0 -- original", diffErr: fmt.Errorf("github 503")}
	svc := validationSvc([]prompt.Ancestor{
		{NodeID: "up.node", ServiceName: "up-svc", LastCommitSHA: "upsha", LastRepo: "o/up-repo", FilePath: "models/up.sql", Depth: 1},
	}, src)
	svc.LLM = llm

	r, err := validationFixer{}.Propose(context.Background(), svc, validationInput())
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed {
		t.Fatalf("status = %v want proposed", r.Proposal.Status)
	}
	if strings.Contains(llm.requests[0].User, "```diff") {
		t.Fatalf("failed diff fetch must not add a diff block:\n%s", llm.requests[0].User)
	}
}

// TestValidation_UpstreamDiffs_CapsAttemptsOnError verifies the five-fetch cap
// counts attempts, not successes: when every eligible ancestor's diff read fails,
// the loop still issues at most maxUpstreamDiffs CommitFileDiff calls rather than
// one per ancestor. This bounds the blast radius of a GitHub outage over a wide
// ancestry on a serial consumer.
func TestValidation_UpstreamDiffs_CapsAttemptsOnError(t *testing.T) {
	src := &fakeSource{content: "SELECT 0 -- original", diffErr: fmt.Errorf("github 503")}
	var ancestors []prompt.Ancestor
	for i := 0; i < 8; i++ {
		ancestors = append(ancestors, prompt.Ancestor{
			NodeID: fmt.Sprintf("up.%d", i), ServiceName: "up-svc", LastCommitSHA: "s",
			LastRepo: "o/up-repo", FilePath: fmt.Sprintf("models/up%d.sql", i), Depth: 1,
		})
	}
	svc := validationSvc(ancestors, src)

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if len(src.diffPaths) != maxUpstreamDiffs {
		t.Fatalf("attempted %d fetches on all-error ancestry, want cap of %d", len(src.diffPaths), maxUpstreamDiffs)
	}
}

// TestValidation_UpstreamDiffs_SanitizesPatch verifies the upstream patch is run
// through the LogSanitizer before it is embedded in the prompt, so a secret in a
// patched line is redacted rather than sent to the external LLM.
func TestValidation_UpstreamDiffs_SanitizesPatch(t *testing.T) {
	llm := &fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "SELECT 1 -- candidate", Confidence: "high"},
		{ProposedSQL: "SELECT 1 -- source", Confidence: "high"},
	}}
	src := &fakeSource{content: "SELECT 0 -- original", diff: "@@ -1 +1 @@\n-old\n+password = SEKRET"}
	svc := validationSvc([]prompt.Ancestor{
		{NodeID: "up.node", ServiceName: "up-svc", LastCommitSHA: "upsha", LastRepo: "o/up-repo", FilePath: "models/up.sql", Depth: 1},
	}, src)
	svc.LLM = llm
	svc.Sanitizer = redactingSanitizer{secret: "SEKRET", marker: "[redacted]"}

	if _, err := (validationFixer{}).Propose(context.Background(), svc, validationInput()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(llm.requests[0].User, "SEKRET") {
		t.Fatalf("unsanitized secret leaked into the prompt:\n%s", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, "[redacted]") {
		t.Fatalf("sanitized marker missing from the embedded diff:\n%s", llm.requests[0].User)
	}
}
