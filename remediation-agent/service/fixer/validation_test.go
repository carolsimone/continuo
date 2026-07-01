package fixer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
	queue []ports.ProposeResult
	errs  []error
	calls int
}

func (f *fakeLLM) Propose(_ context.Context, _ ports.ProposeRequest) (ports.ProposeResult, error) {
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

// fakeSource returns a fixed file content or an error. readPath records the
// last path argument passed to ReadFile so tests can assert the full path.
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
		CandidateSQLURI: "s3://cand", DBTLog: "boom", Attempt: 1}
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
		CandidateSQLURI: "s3://cand", DBTLog: "boom", Attempt: 1}
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
		CandidateSQLURI: "s3://cand", DBTLog: "boom", Attempt: 1}
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
		CandidateSQLURI: "s3://cand", DBTLog: "boom", Attempt: 1}
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
