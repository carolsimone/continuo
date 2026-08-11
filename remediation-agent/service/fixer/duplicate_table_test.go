package fixer

import (
	"context"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

func TestDuplicateTable_ReadsOnlyTheChangedFileAndProposesARename(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{"services/marketing/models/orders.sql": "select 1 as id"}}
	llm := &fakeLLM{queue: []ports.ProposeResult{{
		TargetFile:      "services/marketing/models/orders.sql",
		ProposedContent: "{{ config(alias='marketing_orders') }}\nselect 1 as id",
		Confidence:      "high",
		Rationale:       "renamed to avoid the collision with finance",
	}}}
	svc := Services{
		Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"marketing": "services/marketing", "finance": "services/finance"},
	}
	in := Input{
		Source: "duplicate_table", ReleaseID: "rel-1", NodeID: "analytics.orders_v2", RelationID: "analytics.orders",
		Repo: "owner/repo", CommitSHA: "abc123", Service: "marketing", FilePath: "models/orders.sql",
		NodeType: "dbt-model", OtherService: "finance", OtherFilePath: "services/finance/models/orders.sql", Attempt: 1,
	}
	r, err := duplicateTableFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusProposed || r.Proposal.FilePath != "services/marketing/models/orders.sql" {
		t.Fatalf("got status=%v file=%q", r.Proposal.Status, r.Proposal.FilePath)
	}
	if got := fs.readPaths(); len(got) != 1 || got[0] != "services/marketing/models/orders.sql" {
		t.Fatalf("readPaths = %v, want only the changed claimant (the competing service's source must not be read)", got)
	}
	if len(llm.requests) != 1 {
		t.Fatalf("expected 1 LLM call, got %d", len(llm.requests))
	}
	// The prompt must name the contested RELATION, not the target's own node
	// id — the two differ here because the target carries no alias override
	// yet while the relation ("analytics.orders") is the name both claimants
	// currently race for.
	if !strings.Contains(llm.requests[0].User, "analytics.orders") {
		t.Fatalf("prompt missing the contested relation: %q", llm.requests[0].User)
	}
	if strings.Contains(llm.requests[0].User, "analytics.orders_v2") {
		t.Fatalf("prompt must name the contested relation, not the target's own node id: %q", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, "finance") {
		t.Fatalf("prompt missing the competing service: %q", llm.requests[0].User)
	}
	if !strings.Contains(llm.requests[0].User, "services/finance/models/orders.sql") {
		t.Fatalf("prompt missing the competing claimant's file path: %q", llm.requests[0].User)
	}
}

// TestDuplicateTable_SkipsAPythonTarget proves the fixer never reads or
// prompts for a python claimant: its relation is declared in the service's
// contract.yaml, whose repository path this system does not carry, so the
// script named by FilePath cannot contain the fix. Before the NodeType check
// existed, this Fixer read the script (the fake source map below would have
// served it) and would have proposed a cosmetic, non-fixing edit.
func TestDuplicateTable_SkipsAPythonTarget(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{"services/marketing/models/orders.py": "print('hello')"}}
	llm := &fakeLLM{}
	svc := Services{
		Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"marketing": "services/marketing", "finance": "services/finance"},
	}
	in := Input{
		Source: "duplicate_table", ReleaseID: "rel-1", NodeID: "analytics.orders",
		Repo: "owner/repo", CommitSHA: "abc123", Service: "marketing", FilePath: "models/orders.py",
		NodeType: "python-model", OtherService: "finance", Attempt: 1,
	}
	r, err := duplicateTableFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if got := fs.readPaths(); len(got) != 0 {
		t.Fatalf("readPaths = %v, want no read for a python target", got)
	}
	if len(llm.requests) != 0 {
		t.Fatalf("expected no LLM call for a python target, got %d", len(llm.requests))
	}
}

// TestDuplicateTable_SkipsASeedTarget proves the fixer never reads or prompts
// for a dbt-seed claimant: a seed's relation name comes from its CSV filename
// or project config, never from the CSV's own contents, so the file named by
// FilePath cannot contain the fix — and accepting an edited CSV as a rename
// proposal would mean accepting an edit to the seed's DATA, not its identity.
func TestDuplicateTable_SkipsASeedTarget(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{"services/marketing/seeds/orders.csv": "id,name\n1,a"}}
	llm := &fakeLLM{}
	svc := Services{
		Source: fs, LLM: llm, Evidence: fakeEvidence{}, Sanitizer: fakeSanitizer{},
		Artifacts: &fakeArtifacts{}, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"marketing": "services/marketing", "finance": "services/finance"},
	}
	in := Input{
		Source: "duplicate_table", ReleaseID: "rel-1", NodeID: "analytics.orders",
		Repo: "owner/repo", CommitSHA: "abc123", Service: "marketing", FilePath: "seeds/orders.csv",
		NodeType: "dbt-seed", OtherService: "finance", Attempt: 1,
	}
	r, err := duplicateTableFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
	if got := fs.readPaths(); len(got) != 0 {
		t.Fatalf("readPaths = %v, want no read for a seed target", got)
	}
	if len(llm.requests) != 0 {
		t.Fatalf("expected no LLM call for a seed target, got %d", len(llm.requests))
	}
}

func TestDuplicateTable_SkipsWithoutASourceLocation(t *testing.T) {
	svc := Services{Logger: testLogger()}
	in := Input{Source: "duplicate_table", ReleaseID: "rel-1", NodeID: "analytics.orders", Attempt: 1}
	r, err := duplicateTableFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

func TestDuplicateTable_SkipsWhenTheServiceHasNoRepoMapping(t *testing.T) {
	svc := Services{Logger: testLogger(), ServiceRepoPaths: map[string]string{"finance": "services/finance"}}
	in := Input{Source: "duplicate_table", ReleaseID: "rel-1", NodeID: "analytics.orders",
		Service: "marketing", FilePath: "models/orders.sql", Attempt: 1}
	r, err := duplicateTableFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatal(err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

// TestDuplicateTable_SkipsWhenTheTargetIsInAnotherTeamsRepo proves the
// production shape of a bootstrap collision: each team ships from its own
// repository, so a target claimant outside the changed service is not present
// at this release's commit. The fake source map returns ErrSourceNotFound for
// any path it was not given, exactly as the real GitHub reader does for a file
// absent at that commit. Skip rather than propose a change to a file that
// could not be read.
func TestDuplicateTable_SkipsWhenTheTargetIsInAnotherTeamsRepo(t *testing.T) {
	fs := &fakeSourceMap{files: map[string]string{}}
	svc := Services{Source: fs, Logger: testLogger(),
		ServiceRepoPaths: map[string]string{"finance": "services/finance", "marketing": "services/marketing"}}
	in := Input{
		Source: "duplicate_table", ReleaseID: "rel-1", NodeID: "analytics.orders",
		Repo: "acme/marketing", CommitSHA: "abc123", Service: "finance", FilePath: "models/orders.sql",
		OtherService: "marketing", Attempt: 1,
	}
	r, err := duplicateTableFixer{}.Propose(context.Background(), svc, in)
	if err != nil {
		t.Fatalf("an unreadable target is a skip, not a redelivery: %v", err)
	}
	if r.Proposal.Status != proposal.StatusSkipped {
		t.Fatalf("status = %v want skipped", r.Proposal.Status)
	}
}

func TestFor_ResolvesTheDuplicateTableClass(t *testing.T) {
	fx, err := For("duplicate_table")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fx.(duplicateTableFixer); !ok {
		t.Fatalf("For(duplicate_table) = %T, want duplicateTableFixer", fx)
	}
}
