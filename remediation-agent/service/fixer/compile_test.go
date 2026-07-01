package fixer

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// fakeSourceMap is an in-memory SourceReader keyed by full repo-relative path,
// used by the compileFixer tests to control both ReadFile and ListDir results.
type fakeSourceMap struct {
	files   map[string]string
	dir     map[string][]string
	readErr error
}

func (f *fakeSourceMap) ReadFile(_ context.Context, _, _, path string) (string, error) {
	if f.readErr != nil {
		return "", f.readErr
	}
	c, ok := f.files[path]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

func (f *fakeSourceMap) ListDir(_ context.Context, _, _, d string) ([]string, error) {
	paths, ok := f.dir[d]
	if !ok {
		return nil, ports.ErrSourceNotFound
	}
	return paths, nil
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
		Source:    fs,
		LLM:       &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/x.sql", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high", Rationale: "fix jinja"}}},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLog: "Compilation Error", ReleaseID: "r", Attempt: 1}
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
		Source:    fs,
		LLM:       &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/schema.yml", ProposedContent: "version: 2\nmodels: [{name: x}]", Confidence: "high"}}},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLog: "Compilation Error", ReleaseID: "r", Attempt: 1}
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
		Source:    fs,
		LLM:       &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/evil.sql", ProposedContent: "drop table", Confidence: "high"}}},
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
		Source:    fs,
		LLM:       &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "services/svc/models/x.sql", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high", Rationale: "fix jinja"}}},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLog: "Compilation Error", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("got status=%v file=%q, want proposed on the offending file despite failed context reads", r.Proposal.Status, r.Proposal.FilePath)
	}
}

// TestCompile_EmptyTargetFile_DefaultsToPrimary proves that when the model
// returns an empty target_file, compileInterpret defaults it to the offending
// (Primary) file rather than skipping.
func TestCompile_EmptyTargetFile_DefaultsToPrimary(t *testing.T) {
	fs := &fakeSourceMap{
		files: map[string]string{"services/svc/models/x.sql": "{{ config(x), y) }}\nselect 1"},
		dir:   map[string][]string{"services/svc/models": {"services/svc/models/x.sql"}},
	}
	svc := Services{
		Source:    fs,
		LLM:       &fakeLLM{queue: []ports.ProposeResult{{TargetFile: "", ProposedContent: "{{ config(x) }}\nselect 1", Confidence: "high", Rationale: "fix jinja"}}},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	in := Input{Source: "compile", NodeID: "svc", Repo: "o/repo", CommitSHA: "sha",
		FilePath: "models/x.sql", DBTLog: "Compilation Error", ReleaseID: "r", Attempt: 1}
	r, err := compileFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/svc/models/x.sql" {
		t.Fatalf("got status=%v file=%q, want proposed defaulting to the offending file", r.Proposal.Status, r.Proposal.FilePath)
	}
}
