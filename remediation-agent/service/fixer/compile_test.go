package fixer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// fakeSourceMap is an in-memory SourceReader keyed by full repo-relative path,
// used by the compileFixer tests to control both ReadFile and ListDir results.
// reads records every path passed to ReadFile, in call order, so a test can
// assert exactly which files a Fixer touched.
type fakeSourceMap struct {
	files   map[string]string
	dir     map[string][]string
	readErr error
	reads   []string
}

func (f *fakeSourceMap) ReadFile(_ context.Context, _, _, path string) (string, error) {
	f.reads = append(f.reads, path)
	if f.readErr != nil {
		return "", f.readErr
	}
	c, ok := f.files[path]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

// readPaths returns every path ReadFile was called with, in call order.
func (f *fakeSourceMap) readPaths() []string {
	return f.reads
}

func (f *fakeSourceMap) ListDir(_ context.Context, _, _, d string) ([]string, error) {
	paths, ok := f.dir[d]
	if !ok {
		return nil, ports.ErrSourceNotFound
	}
	return paths, nil
}

func (f *fakeSourceMap) CommitFileDiff(_ context.Context, _, _, _ string) (string, error) {
	return "", ports.ErrSourceNotFound
}

func TestCompile_EmptyFilePath_Skips(t *testing.T) {
	svc := Services{Source: &fakeSource{}, Logger: testLogger(), ServiceRepoPaths: map[string]string{"svc": "services/svc"}}
	r, err := compileFixer{}.Propose(context.Background(), svc, Input{Source: "compile", NodeID: "svc", FilePath: ""})
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

func TestCompile_OffendingSQL_ProposesToSQL(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "{{ config(x), y) }}\nselect 1"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/x.sql", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high", Rationale: "fix jinja"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("got status=%v file=%q", r.Proposal.Status, r.Proposal.FilePath)
	}
}

func TestCompile_TargetsCoLocatedYAML(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{
			"services/svc/models/x.sql":      "select 1",
			"services/svc/models/schema.yml": "version: 2\nmodels: [{name: x, tests: [bad]}]",
			"services/svc/dbt_project.yml":   "name: svc",
		},
		dir: map[string][]string{"services/svc/models": {"services/svc/models/x.sql", "services/svc/models/schema.yml"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/schema.yml", ProposedContent: "version: 2\nmodels: [{name: x}]", Confidence: "high"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.FilePath != "services/svc/models/schema.yml" {
		t.Fatalf("FilePath = %q, want the yaml", r.Proposal.FilePath)
	}
}

func TestCompile_LLMNamesUnshownFile_Skips(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "select 1"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/evil.sql", ProposedContent: "drop table", Confidence: "high"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha", FilePath: "models/x.sql", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped (arbitrary path rejected)", r.Proposal.Status)
	}
}

func TestCompile_OffendingReadOtherError_ReturnsErr(t *testing.T) {
	svc := Services{Source: &fakeSourceMap{readErr: errors.New("502 bad gateway")}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"}}
	_, err := compileFixer{}.Propose(context.Background(), svc, Input{Source: "compile", NodeID: "svc", FilePath: "models/x.sql", Repo: "o/repo", CommitSHA: "sha"})
	if err == nil {
		t.Fatal("non-404 read error must be returned so the message is redelivered")
	}
}

func TestCompile_OffendingRead404_Skips(t *testing.T) {
	svc := Services{Source: &fakeSourceMap{readErr: ports.ErrSourceNotFound}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"}}
	r, err := compileFixer{}.Propose(context.Background(), svc, Input{Source: "compile", NodeID: "svc", FilePath: "models/x.sql", Repo: "o/repo", CommitSHA: "sha"})
	if err != nil {
		t.Fatalf("404 must not be returned as error: %v", err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

// TestCompile_BestEffortReadsFail_StillProposes proves that a failed co-located
// context read (ListDir 404, missing schema.yml/dbt_project.yml) is swallowed:
// the gather still succeeds using the offending file alone.
func TestCompile_BestEffortReadsFail_StillProposes(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "{{ config(x), y) }}\nselect 1"},
		dir:   map[string][]string{}, // "services/svc/models" absent -> ListDir returns ErrSourceNotFound
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/x.sql", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high", Rationale: "fix jinja"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("got status=%v file=%q, want proposed on the offending file despite failed context reads", r.Proposal.Status, r.Proposal.FilePath)
	}
}

// TestCompile_RelativeTargetFile_SuffixMatch_Proposes proves resolveTarget
// accepts a differently-rooted spelling of a shown file: the model returns the
// project-relative "models/x.sql" while the file was shown as the full repo path
// "services/svc/models/x.sql". The fix is proposed against the shown file, not
// skipped.
func TestCompile_RelativeTargetFile_SuffixMatch_Proposes(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "{{ config(x), y) }}\nselect 1"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "models/x.sql", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("got status=%v file=%q, want proposed against the suffix-matched shown file", r.Proposal.Status, r.Proposal.FilePath)
	}
}

// TestCompile_EmptyTargetMultipleFilesShown_Skips proves that when the model
// omits target_file and more than one file was shown, the intended file is
// ambiguous, so singleFileInterpret skips rather than writing the returned
// content against the offending .sql (which could be content meant for a co-located
// schema.yml).
func TestCompile_EmptyTargetMultipleFilesShown_Skips(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{
			"services/svc/models/x.sql":      "select 1",
			"services/svc/models/schema.yml": "version: 2",
			"services/svc/dbt_project.yml":   "name: svc",
		},
		dir: map[string][]string{"services/svc/models": {"services/svc/models/x.sql", "services/svc/models/schema.yml"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "", ProposedContent: "version: 2\nmodels: []", Confidence: "high"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped (empty target with multiple files shown is ambiguous)", r.Proposal.Status)
	}
}

// TestCompile_LowConfidence_Fails proves a low-confidence compile answer is
// recorded failed even when it changed the file — the model's signal that it
// could not determine a safe fix must not become a proposal.
func TestCompile_LowConfidence_Fails(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "{{ config(x), y) }}\nselect 1"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/x.sql", ProposedContent: "select 999 -- guess", Confidence: "low"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusFailed {
		t.Fatalf("status = %v want failed (low-confidence must not be proposed)", r.Proposal.Status)
	}
}

// TestCompile_SanitizesSourceBeforeLLM proves the offending file content is run
// through the LogSanitizer seam before it is sent to the LLM: a secret in the
// source appears redacted in the prompt, while the diff base stays raw.
func TestCompile_SanitizesSourceBeforeLLM(t *testing.T) {
	const secret = "SECRET_TOKEN_123"
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "select '" + secret + "' as t"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	llm := &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/x.sql", ProposedContent: "select 1 as t", Confidence: "high"}}}
	svc := Services{
		Source:   fs,
		LLM:      llm,
		Evidence: fakeEvidence{}, Sanitizer: redactingSanitizer{secret: secret, marker: "[REDACTED]"},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	_, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(llm.requests) != 1 {
		t.Fatalf("expected 1 LLM call, got %d", len(llm.requests))
	}
	if strings.Contains(llm.requests[0].User, secret) {
		t.Fatal("raw secret was sent to the LLM; source must be sanitized first")
	}
	if !strings.Contains(llm.requests[0].User, "[REDACTED]") {
		t.Fatal("sanitized marker not found in the prompt; source did not pass through the sanitizer")
	}
}

// TestCompile_EmptyTargetFile_DefaultsToPrimary proves that when the model
// returns an empty target_file, singleFileInterpret defaults it to the offending
// (Primary) file rather than skipping.
func TestCompile_EmptyTargetFile_DefaultsToPrimary(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "{{ config(x), y) }}\nselect 1"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	svc := Services{
		Source:   fs,
		LLM:      &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high", Rationale: "fix jinja"}}},
		Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLogURI: "s3://log", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("got status=%v file=%q, want proposed defaulting to the offending file", r.Proposal.Status, r.Proposal.FilePath)
	}
}
