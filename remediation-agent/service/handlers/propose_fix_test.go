package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/remediation-agent/domain/event"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// fakeEvidence returns pre-loaded strings by URI, or an error if set.
type fakeEvidence struct {
	vals map[string]string
	err  error
}

func (f fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.vals[uri], nil
}

// fakeAncestry returns a fixed ancestor slice, or an error if set.
type fakeAncestry struct {
	a   []prompt.Ancestor
	err error
}

func (f fakeAncestry) Ancestors(_ context.Context, _ string) ([]prompt.Ancestor, error) {
	return f.a, f.err
}

// fakeSanitizer is a pass-through log sanitizer.
type fakeSanitizer struct{}

func (fakeSanitizer) Sanitize(s string) string { return s }

// fakeLLM returns a pre-loaded result, or an error if set.
type fakeLLM struct {
	res ports.ProposeResult
	err error
}

func (f fakeLLM) Propose(_ context.Context, _ ports.ProposeRequest) (ports.ProposeResult, error) {
	return f.res, f.err
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

// fakeClock returns a fixed UTC timestamp.
type fakeClock struct{}

func (fakeClock) Now() time.Time { return time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC) }

// fakeProposalRepo satisfies repository.ProposalRepository in memory.
type fakeProposalRepo struct {
	count    int
	inserted []proposal.Proposal
}

func (r *fakeProposalRepo) CountAttempts(_ context.Context, _, _, _ string) (int, error) {
	return r.count, nil
}

func (r *fakeProposalRepo) Insert(_ context.Context, p proposal.Proposal) error {
	r.inserted = append(r.inserted, p)
	return nil
}

// fakeOutbox satisfies outbox.Repository in memory. Create takes a pointer to
// match the real pkg/outbox.Repository interface. The read-path and retry
// methods are no-ops as they are not exercised by ProposeFix.
type fakeOutbox struct {
	entries []*outbox.Entry
}

func (o *fakeOutbox) Create(_ context.Context, e *outbox.Entry) error {
	o.entries = append(o.entries, e)
	return nil
}

func (o *fakeOutbox) GetPendingBatch(_ context.Context, _ int) ([]*outbox.Entry, error) {
	return nil, nil
}

func (o *fakeOutbox) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }

func (o *fakeOutbox) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }

func (o *fakeOutbox) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error { return nil }

func (o *fakeOutbox) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

// fakeUoW satisfies uow.UnitOfWork in memory.
type fakeUoW struct {
	pr        *fakeProposalRepo
	ob        *fakeOutbox
	committed bool
}

func (u *fakeUoW) Begin(context.Context) error                       { return nil }
func (u *fakeUoW) Commit() error                                     { u.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                   { return nil }
func (u *fakeUoW) ProposalRepo() repository.ProposalRepository       { return u.pr }
func (u *fakeUoW) OutboxRepo() outbox.Repository                     { return u.ob }

func deps(u *fakeUoW, ev fakeEvidence, llm fakeLLM, art *fakeArtifacts) Deps {
	return Deps{
		NewUoW:      func() uow.UnitOfWork { return u },
		LLM:         llm,
		Evidence:    ev,
		Ancestry:    fakeAncestry{},
		Sanitizer:   fakeSanitizer{},
		Artifacts:   art,
		Clock:       fakeClock{},
		Logger:      slog.Default(),
		MaxAttempts: 3,
		Bucket:      "bucket",
	}
}

func baseTrigger() Trigger {
	return Trigger{
		Source:          "validation",
		ReleaseID:       "r1",
		NodeID:          "s.n",
		ErrorSignature:  "sig",
		Category:        "logic",
		DBTLogURI:       "s3://b/log",
		CandidateSQLURI: "s3://b/sql",
		Repo:            "o/r",
		CommitSHA:       "abc",
	}
}

func TestProposeFix_HappyPath(t *testing.T) {
	u := &fakeUoW{pr: &fakeProposalRepo{count: 0}, ob: &fakeOutbox{}}
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := fakeLLM{res: ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}}
	art := &fakeArtifacts{}

	if err := ProposeFix(context.Background(), deps(u, ev, llm, art), baseTrigger()); err != nil {
		t.Fatal(err)
	}

	if len(u.pr.inserted) != 1 || u.pr.inserted[0].Status != proposal.StatusProposed {
		t.Fatalf("expected one proposed row, got %+v", u.pr.inserted)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected one outbox entry, got %d", len(u.ob.entries))
	}
	var p event.RemediationProposed
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if p.NodeID != "s.n" || p.Confidence != "high" || p.EventID != event.RemediationEventID("r1", "s.n", 1).String() {
		t.Fatalf("bad payload %+v", p)
	}
	if len(art.written) != 2 {
		t.Fatalf("expected 2 artifacts (.sql + .diff), got %d", len(art.written))
	}
	if !u.committed {
		t.Fatal("not committed")
	}
}

func TestProposeFix_AttemptCapEscalates(t *testing.T) {
	u := &fakeUoW{pr: &fakeProposalRepo{count: 3}, ob: &fakeOutbox{}}
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}

	if err := ProposeFix(context.Background(), deps(u, ev, fakeLLM{}, &fakeArtifacts{}), baseTrigger()); err != nil {
		t.Fatal(err)
	}
	if len(u.pr.inserted) != 1 || u.pr.inserted[0].Status != proposal.StatusEscalated {
		t.Fatalf("expected escalated row, got %+v", u.pr.inserted)
	}
	if len(u.ob.entries) != 0 {
		t.Fatal("escalated must not emit an outbox entry")
	}
}

func TestProposeFix_EmptyCandidateSQLSkips(t *testing.T) {
	u := &fakeUoW{pr: &fakeProposalRepo{}, ob: &fakeOutbox{}}
	tr := baseTrigger()
	tr.CandidateSQLURI = ""

	if err := ProposeFix(context.Background(), deps(u, fakeEvidence{}, fakeLLM{}, &fakeArtifacts{}), tr); err != nil {
		t.Fatal(err)
	}
	if len(u.pr.inserted) != 1 || u.pr.inserted[0].Status != proposal.StatusSkipped {
		t.Fatalf("expected skipped row, got %+v", u.pr.inserted)
	}
	if len(u.ob.entries) != 0 {
		t.Fatal("skip must not emit an outbox entry")
	}
}

func TestProposeFix_LLMEmptyFails(t *testing.T) {
	u := &fakeUoW{pr: &fakeProposalRepo{}, ob: &fakeOutbox{}}
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}
	llm := fakeLLM{res: ports.ProposeResult{ProposedSQL: ""}}

	if err := ProposeFix(context.Background(), deps(u, ev, llm, &fakeArtifacts{}), baseTrigger()); err != nil {
		t.Fatal(err)
	}
	if len(u.pr.inserted) != 1 || u.pr.inserted[0].Status != proposal.StatusFailed {
		t.Fatalf("expected failed row, got %+v", u.pr.inserted)
	}
	if len(u.ob.entries) != 0 {
		t.Fatal("failed must not emit an outbox entry")
	}
}
