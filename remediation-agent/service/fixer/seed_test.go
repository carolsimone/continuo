package fixer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

func TestSeed_FixableCSV_Proposes(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{"services/svc/seeds/ref.csv": "id,name\n1,a,b"}}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{ProposedContent: "id,name\n1,\"a,b\"", Confidence: "high", Rationale: "quote the comma"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "seed_build", NodeID: "analytics.ref", Service: "svc", FilePath: "seeds/ref.csv",
		Repo: "o/repo", CommitSHA: "sha", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := seedFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/seeds/ref.csv" {
		t.Fatalf("got status=%v file=%q", r.Proposal.Status, r.Proposal.FilePath)
	}
}

func TestSeed_UninferableValue_LowConfidenceUnchanged_Fails(t *testing.T) {
	original := "id,amount\n1,NaN"
	fs := &fakeSourceMap{files: map[string]string{"services/svc/seeds/ref.csv": original}}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{ProposedContent: original, Confidence: "low", Rationale: "cannot infer amount"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "seed_build", NodeID: "analytics.ref", Service: "svc", FilePath: "seeds/ref.csv",
		Repo: "o/repo", CommitSHA: "sha", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := seedFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status == proposal.StatusProposed {
		t.Fatalf("unchanged low-confidence CSV must not be proposed; got %v", r.Proposal.Status)
	}
}

// TestSeed_LowConfidenceChanged_Fails proves that a CHANGED CSV returned with
// low confidence is recorded failed, not proposed: the model could not infer the
// bad value, so its guess must not become a fix that invents warehouse data.
func TestSeed_LowConfidenceChanged_Fails(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{"services/svc/seeds/ref.csv": "id,amount\n1,NaN"}}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{ProposedContent: "id,amount\n1,0", Confidence: "low", Rationale: "guessed amount"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "seed_build", NodeID: "analytics.ref", Service: "svc", FilePath: "seeds/ref.csv",
		Repo: "o/repo", CommitSHA: "sha", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := seedFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusFailed {
		t.Fatalf("status = %v want failed (low-confidence changed CSV must not be proposed)", r.Proposal.Status)
	}
}

// TestSeed_SanitizesCSVBeforeLLM proves the CSV is run through the LogSanitizer
// seam before it is sent to the LLM.
func TestSeed_SanitizesCSVBeforeLLM(t *testing.T) {
	const secret = "555-11-2222"
	fs := &fakeSourceMap{files: map[string]string{"services/svc/seeds/ref.csv": "id,ssn\n1," + secret}}
	llm := &fakeLLM{queue: []ports.ProposeResult{{ProposedContent: "id,ssn\n1,\"" + secret + "\"", Confidence: "high"}}}
	svc := Services{
		Source:   fs,
		LLM:      llm,
		Evidence: fakeEvidence{}, Sanitizer: redactingSanitizer{secret: secret, marker: "[REDACTED]"},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "seed_build", NodeID: "analytics.ref", Service: "svc", FilePath: "seeds/ref.csv",
		Repo: "o/repo", CommitSHA: "sha", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	_, err := seedFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(llm.requests) != 1 {
		t.Fatalf("expected 1 LLM call, got %d", len(llm.requests))
	}
	if strings.Contains(llm.requests[0].User, secret) {
		t.Fatal("raw secret was sent to the LLM; CSV must be sanitized first")
	}
}

// TestSeed_FallsBackToAncestry_WhenServiceMissing proves that when in.Service
// is empty, seedGather fills it (and any missing FilePath) from
// Ancestry.NodeContext before resolving the repo path, mirroring the
// compile/validation fixers' priority (trigger evidence first, Ancestry as
// fallback).
func TestSeed_FallsBackToAncestry_WhenServiceMissing(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{"services/svc/seeds/ref.csv": "id,name\n1,a,b"}}
	svc := Services{
		Source:   fs,
		Ancestry: fakeAncestry{filePath: "seeds/ref.csv", service: "svc"},
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{ProposedContent: "id,name\n1,\"a,b\"", Confidence: "high"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "seed_build", NodeID: "analytics.ref", FilePath: "seeds/ref.csv",
		Repo: "o/repo", CommitSHA: "sha", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := seedFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/seeds/ref.csv" {
		t.Fatalf("got status=%v file=%q, want proposed via the ancestry-resolved service", r.Proposal.Status, r.Proposal.FilePath)
	}
}

// TestSeed_AncestryError_Skips proves that an Ancestry error (used only when
// FilePath or Service is empty) is treated as a skip, not a hard error: the
// same-degrade-gracefully rule as the other fixers' best-effort Ancestry use.
func TestSeed_AncestryError_Skips(t *testing.T) {
	svc := Services{
		Ancestry: fakeAncestry{err: errors.New("ancestry unavailable")},
		Logger:   testLogger(),
	}
	in := Input{Source: "seed_build", NodeID: "analytics.ref", Repo: "o/repo", CommitSHA: "sha"}
	r, err := seedFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatalf("ancestry error must not be returned as a hard error: %v", err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

func TestSeed_CSVRead404_Skips(t *testing.T) {
	svc := Services{Source: &fakeSourceMap{readErr: ports.ErrSourceNotFound}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"}}
	r, err := seedFixer{}.Propose(context.Background(), svc, Input{Source: "seed_build", NodeID: "analytics.ref", Service: "svc", FilePath: "seeds/ref.csv", Repo: "o/repo", CommitSHA: "sha"})
	if err != nil {
		t.Fatalf("404 must not error: %v", err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}
