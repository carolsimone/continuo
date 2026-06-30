package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
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

// fakeAncestry returns a fixed file path, service name, and ancestor slice,
// or an error if set.
type fakeAncestry struct {
	fp  string
	svc string
	a   []prompt.Ancestor
	err error
}

func (f fakeAncestry) NodeContext(_ context.Context, _ string) (string, string, []prompt.Ancestor, error) {
	return f.fp, f.svc, f.a, f.err
}

// fakeSanitizer is a pass-through log sanitizer.
type fakeSanitizer struct{}

func (fakeSanitizer) Sanitize(s string) string { return s }

// fakeLLM returns results from a queue (one per Propose call, in order).
// When the queue is exhausted, the last entry is repeated. A single-entry queue
// reproduces the original single-result behaviour.
type fakeLLM struct {
	queue []ports.ProposeResult
	errs  []error
	calls int
}

func newFakeLLM(res ports.ProposeResult, err error) fakeLLM {
	return fakeLLM{queue: []ports.ProposeResult{res}, errs: []error{err}}
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

func (r *fakeProposalRepo) Get(_ context.Context, _ string) (proposal.View, error) {
	return proposal.View{}, repository.ErrNotFound
}

func (r *fakeProposalRepo) List(_ context.Context, _ repository.ProposalFilter) ([]proposal.View, error) {
	return nil, nil
}

func (r *fakeProposalRepo) BeginPR(_ context.Context, _, _ string) (proposal.PRClaim, error) {
	return proposal.PRClaim{}, nil
}

func (r *fakeProposalRepo) RecordPR(_ context.Context, _ string, _ string, _ int, _ string, _ time.Time) error {
	return nil
}

func (r *fakeProposalRepo) FailPR(_ context.Context, _ string) error {
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

// fakeMsgProcRepo satisfies messageprocessing.Repository in memory.
// InsertIfNotExists returns (newUUID, true, nil) on the first call for a given
// (messageID, outboxEntryID) combination, and (existingUUID, false, nil) on
// subsequent calls for the same combination, mimicking the DB unique-constraint
// dedup behaviour.
type fakeMsgProcRepo struct {
	// seen maps the dedup key to the assigned UUID (populated on first insert).
	seen map[string]uuid.UUID
	// rows stores inserted rows keyed by UUID for GetByID.
	rows map[uuid.UUID]*messageprocessing.MessageProcessing
}

func newFakeMsgProcRepo() *fakeMsgProcRepo {
	return &fakeMsgProcRepo{
		seen: map[string]uuid.UUID{},
		rows: map[uuid.UUID]*messageprocessing.MessageProcessing{},
	}
}

// dedupKey produces the string used to detect a repeat call. It combines
// messageID and outboxEntryID (if non-nil) so both dedup axes are covered.
func (r *fakeMsgProcRepo) dedupKey(m *messageprocessing.MessageProcessing) string {
	if m.OutboxEntryID != nil {
		return "oe:" + m.OutboxEntryID.String()
	}
	return "mid:" + m.MessageID + ":" + m.StreamName
}

func (r *fakeMsgProcRepo) InsertIfNotExists(
	ctx context.Context, m *messageprocessing.MessageProcessing,
) (uuid.UUID, bool, error) {
	key := r.dedupKey(m)
	if id, exists := r.seen[key]; exists {
		return id, false, nil
	}
	id := uuid.New()
	r.seen[key] = id
	stored := *m
	stored.ID = id
	r.rows[id] = &stored
	return id, true, nil
}

func (r *fakeMsgProcRepo) GetByMessageIDAndStream(
	_ context.Context, messageID, streamName string,
) (*messageprocessing.MessageProcessing, error) {
	for _, m := range r.rows {
		if m.MessageID == messageID && m.StreamName == streamName {
			return m, nil
		}
	}
	return nil, nil
}

func (r *fakeMsgProcRepo) GetByID(
	_ context.Context, id uuid.UUID,
) (*messageprocessing.MessageProcessing, error) {
	m, ok := r.rows[id]
	if !ok {
		return nil, nil
	}
	return m, nil
}

func (r *fakeMsgProcRepo) UpdateState(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (r *fakeMsgProcRepo) DeleteTerminalOlderThan(
	_ context.Context, _ time.Duration, _ int,
) (int64, error) {
	return 0, nil
}

// fakeUoW satisfies uow.UnitOfWork in memory.
type fakeUoW struct {
	pr        *fakeProposalRepo
	ob        *fakeOutbox
	mp        *fakeMsgProcRepo
	committed bool
}

func (u *fakeUoW) Begin(context.Context) error                         { return nil }
func (u *fakeUoW) Commit() error                                       { u.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                     { return nil }
func (u *fakeUoW) ProposalRepo() repository.ProposalRepository         { return u.pr }
func (u *fakeUoW) OutboxRepo() outbox.Repository                       { return u.ob }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return u.mp }

func newFakeUoW() *fakeUoW {
	return &fakeUoW{
		pr: &fakeProposalRepo{count: 0},
		ob: &fakeOutbox{},
		mp: newFakeMsgProcRepo(),
	}
}

func deps(u *fakeUoW, ev fakeEvidence, llm *fakeLLM, art *fakeArtifacts) Deps {
	return Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              llm,
		Evidence:         ev,
		Ancestry:         fakeAncestry{},
		Source:           &fakeSource{},
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
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
		MessageID:       "1-0",
	}
}

// TestProposeFix_CompileSource verifies the compile branch: a compile trigger
// carries no candidate SQL but a FilePath. The handler must bypass Ancestry,
// read the offending source from version control, prompt the LLM with the dbt
// error + raw source, and persist a non-skipped, source-resolved proposal whose
// FilePath and Source are preserved. A non-empty proposed-fix artifact is written.
func TestProposeFix_CompileSource(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://c.log": "Compilation Error in model daily_transactions (models/daily_transactions.sql)\n  unexpected '}' in config block",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "{{ config(materialized='table', tags=['daily']) }}\nselect * from analytics.raw_transactions",
		Rationale:   "fixed malformed config block",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}
	// The broken source has a malformed config()/tags expression.
	src := &fakeSource{content: "{{ config(materialized='table'), tags=['daily'])}}\nselect * from analytics.raw_transactions"}

	d := Deps{
		NewUoW:      func() uow.UnitOfWork { return u },
		LLM:         &llm,
		Evidence:    ev,
		Ancestry:    fakeAncestry{err: fmt.Errorf("ancestry must not be called for compile")},
		Source:      src,
		Sanitizer:   fakeSanitizer{},
		Artifacts:   art,
		Clock:       fakeClock{},
		Logger:      slog.Default(),
		MaxAttempts: 3,
		Bucket:      "bucket",
		// Service "core" maps to the repo root, so the full path equals FilePath.
		ServiceRepoPaths: map[string]string{"core": ""},
	}
	tr := Trigger{
		Source:          "compile",
		ReleaseID:       "r1",
		NodeID:          "core",
		ErrorSignature:  "compile-err",
		FilePath:        "models/daily_transactions.sql",
		CandidateSQLURI: "",
		DBTLogURI:       "s3://c.log",
		Repo:            "o/r",
		CommitSHA:       "sha",
		MessageID:       "7-0",
	}

	if err := ProposeFix(context.Background(), d, tr); err != nil {
		t.Fatal(err)
	}

	if len(u.pr.inserted) != 1 {
		t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
	}
	p := u.pr.inserted[0]
	if p.Status == proposal.StatusSkipped {
		t.Fatalf("compile proposal must not be skipped, got %+v", p)
	}
	if p.Status != proposal.StatusProposed {
		t.Fatalf("expected proposed status, got %s", p.Status)
	}
	if p.Source != "compile" {
		t.Fatalf("Source = %q, want compile", p.Source)
	}
	if p.FilePath != "models/daily_transactions.sql" {
		t.Fatalf("FilePath = %q, want models/daily_transactions.sql", p.FilePath)
	}
	if !p.SourceResolved {
		t.Fatal("expected SourceResolved=true")
	}
	// Source must be read at join(prefix, FilePath); prefix is empty here.
	if src.readPath != "models/daily_transactions.sql" {
		t.Fatalf("ReadFile path = %q, want models/daily_transactions.sql", src.readPath)
	}
	// At least one non-empty proposed-fix artifact must be written.
	nonEmpty := 0
	for _, body := range art.written {
		if body != "" {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		t.Fatalf("expected a non-empty proposed-fix artifact, got %v", art.written)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox entry, got %d", len(u.ob.entries))
	}
	if !u.committed {
		t.Fatal("not committed")
	}
}

// TestProposeFix_SeedSourceViaThreadedPayload verifies the primary seed_build
// path: when FilePath and Service are carried on the trigger (threaded from the
// candidate topology), the handler must NOT call Ancestry and must produce a
// source-resolved proposal. This is the common case for newly-added seeds that
// do not yet exist in the promoted topology that Ancestry serves.
func TestProposeFix_SeedSourceViaThreadedPayload(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/log": "Database Error in seed customers: extra column",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "id,name\n1,alice",
		Rationale:   "removed extra column",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}
	src := &fakeSource{content: "id,name\n1,alice,extra"}

	d := Deps{
		NewUoW:  func() uow.UnitOfWork { return u },
		LLM:     &llm,
		Evidence: ev,
		// Ancestry must NOT be called: it only has promoted topology, which does
		// not include newly-added seeds. An error here proves we skip it.
		Ancestry:         fakeAncestry{err: fmt.Errorf("ancestry must not be called for new seeds")},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	tr := Trigger{
		Source:          "seed_build",
		ReleaseID:       "r1",
		NodeID:          "svc.customers",
		ErrorSignature:  "seed-err",
		// FilePath and Service are threaded from the candidate topology by
		// release-controller, bypassing the need for an Ancestry call.
		FilePath:        "seeds/customers.csv",
		Service:         "svc",
		CandidateSQLURI: "",
		DBTLogURI:       "s3://b/log",
		Repo:            "o/r",
		CommitSHA:       "sha",
		MessageID:       "8-0",
	}

	if err := ProposeFix(context.Background(), d, tr); err != nil {
		t.Fatal(err)
	}

	if len(u.pr.inserted) != 1 {
		t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
	}
	p := u.pr.inserted[0]
	if p.Status != proposal.StatusProposed {
		t.Fatalf("expected proposed status, got %s", p.Status)
	}
	if p.Source != "seed_build" {
		t.Fatalf("Source = %q, want seed_build", p.Source)
	}
	if !p.SourceResolved {
		t.Fatal("expected SourceResolved=true")
	}
	if p.FilePath != "services/svc/seeds/customers.csv" {
		t.Fatalf("FilePath = %q, want services/svc/seeds/customers.csv", p.FilePath)
	}
	if src.readPath != "services/svc/seeds/customers.csv" {
		t.Fatalf("ReadFile path = %q, want services/svc/seeds/customers.csv", src.readPath)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox entry, got %d", len(u.ob.entries))
	}
}

// TestProposeFix_SeedSourceFallsBackToAncestry verifies the Ancestry fallback
// path for seed_build: when FilePath is absent on the trigger (e.g. an older
// rejection payload that predates the candidate-topology threading), the handler
// falls back to Ancestry to resolve the source location.
func TestProposeFix_SeedSourceFallsBackToAncestry(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/log": "Database Error in seed customers: extra column",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "id,name\n1,alice",
		Rationale:   "removed extra column",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}
	src := &fakeSource{content: "id,name\n1,alice,extra"}

	d := Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              &llm,
		Evidence:         ev,
		Ancestry:         fakeAncestry{fp: "seeds/customers.csv", svc: "svc"},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}
	// No FilePath or Service on the trigger: must fall back to Ancestry.
	tr := Trigger{
		Source:          "seed_build",
		ReleaseID:       "r1",
		NodeID:          "svc.customers",
		ErrorSignature:  "seed-err",
		FilePath:        "",
		Service:         "",
		CandidateSQLURI: "",
		DBTLogURI:       "s3://b/log",
		Repo:            "o/r",
		CommitSHA:       "sha",
		MessageID:       "8-1",
	}

	if err := ProposeFix(context.Background(), d, tr); err != nil {
		t.Fatal(err)
	}

	if len(u.pr.inserted) != 1 {
		t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
	}
	p := u.pr.inserted[0]
	if p.Status != proposal.StatusProposed {
		t.Fatalf("expected proposed status on ancestry fallback, got %s", p.Status)
	}
	if !p.SourceResolved {
		t.Fatal("expected SourceResolved=true on ancestry fallback")
	}
	if p.FilePath != "services/svc/seeds/customers.csv" {
		t.Fatalf("FilePath = %q, want services/svc/seeds/customers.csv", p.FilePath)
	}
}

func TestProposeFix_HappyPath(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}

	if err := ProposeFix(context.Background(), deps(u, ev, &llm, art), baseTrigger()); err != nil {
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
	// Artifact keys must include attempt number.
	if _, ok := art.written["proposed-fix/r1/s.n/attempt-1.sql"]; !ok {
		t.Fatalf("expected attempt-scoped sql key; got keys %v", art.written)
	}
	if _, ok := art.written["proposed-fix/r1/s.n/attempt-1.diff"]; !ok {
		t.Fatalf("expected attempt-scoped diff key; got keys %v", art.written)
	}
	// The outbox entry must carry the message_processing row ID.
	if u.ob.entries[0].MessageProcessingID == nil {
		t.Fatal("outbox entry MessageProcessingID must be set on happy path")
	}
	if !u.committed {
		t.Fatal("not committed")
	}
}

func TestProposeFix_AttemptCapEscalates(t *testing.T) {
	u := newFakeUoW()
	u.pr.count = 3
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}
	llm := newFakeLLM(ports.ProposeResult{}, nil)

	if err := ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), baseTrigger()); err != nil {
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
	u := newFakeUoW()
	tr := baseTrigger()
	tr.CandidateSQLURI = ""
	llm := newFakeLLM(ports.ProposeResult{}, nil)

	if err := ProposeFix(context.Background(), deps(u, fakeEvidence{}, &llm, &fakeArtifacts{}), tr); err != nil {
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
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}
	llm := newFakeLLM(ports.ProposeResult{ProposedSQL: ""}, nil)

	if err := ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), baseTrigger()); err != nil {
		t.Fatal(err)
	}
	if len(u.pr.inserted) != 1 || u.pr.inserted[0].Status != proposal.StatusFailed {
		t.Fatalf("expected failed row, got %+v", u.pr.inserted)
	}
	if len(u.ob.entries) != 0 {
		t.Fatal("failed must not emit an outbox entry")
	}
}

// TestProposeFix_DuplicateTriggerIsDeduped calls ProposeFix twice with
// identical trigger data (same MessageID / OutboxEntryID). The second call
// must be recognised as a duplicate and return nil without inserting a second
// proposal row or enqueuing a second outbox entry.
func TestProposeFix_DuplicateTriggerIsDeduped(t *testing.T) {
	// Both calls share the same UoW factory state (fakeMsgProcRepo is reused
	// across calls, which is what the fake is designed for).
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}
	d := deps(u, ev, &llm, art)

	oeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	tr := baseTrigger()
	tr.MessageID = "42-0"
	tr.OutboxEntryID = &oeID

	// First call: must succeed and produce exactly one proposal + one outbox entry.
	if err := ProposeFix(context.Background(), d, tr); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if len(u.pr.inserted) != 1 {
		t.Fatalf("first call: expected 1 proposal row, got %d", len(u.pr.inserted))
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("first call: expected 1 outbox entry, got %d", len(u.ob.entries))
	}

	// Second call with the same trigger (simulates redelivery).
	if err := ProposeFix(context.Background(), d, tr); err != nil {
		t.Fatalf("second call (dup) must return nil, got: %v", err)
	}

	// Counts must not have grown — the duplicate was suppressed.
	if len(u.pr.inserted) != 1 {
		t.Fatalf("after dup: expected still 1 proposal row, got %d", len(u.pr.inserted))
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("after dup: expected still 1 outbox entry, got %d", len(u.ob.entries))
	}
}

// TestProposeFix_Step2_SourceResolved verifies that when ancestry returns a
// file path and the source reader returns the original SQL, the Step-2 LLM call
// produces source artifacts. The final proposal URIs point to the source
// artifacts; the candidate artifacts are also recorded on the proposal.
func TestProposeFix_Step2_SourceResolved(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	// Step-1 returns the candidate fix; Step-2 returns the corrected source.
	llm := fakeLLM{
		queue: []ports.ProposeResult{
			{ProposedSQL: "select customer_id from t", Rationale: "typo fix", Confidence: "high", Model: "m"},
			{ProposedSQL: "{{ config(materialized='table') }}\nselect customer_id from {{ ref('t') }}", Rationale: "typo fix", Confidence: "high", Model: "m"},
		},
		errs: []error{nil, nil},
	}
	art := &fakeArtifacts{}

	src := &fakeSource{content: "{{ config(materialized='table') }}\nselect custmer_id from {{ ref('t') }}"}
	d := Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              &llm,
		Evidence:         ev,
		Ancestry:         fakeAncestry{fp: "models/table_e.sql", svc: "svc", a: []prompt.Ancestor{}},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}

	if err := ProposeFix(context.Background(), d, baseTrigger()); err != nil {
		t.Fatal(err)
	}

	if len(u.pr.inserted) != 1 {
		t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
	}
	p := u.pr.inserted[0]

	if p.Status != proposal.StatusProposed {
		t.Fatalf("expected proposed status, got %s", p.Status)
	}
	if !p.SourceResolved {
		t.Fatal("expected SourceResolved=true")
	}

	// The source must be read at the full path: prefix + filePath.
	if src.readPath != "services/svc/models/table_e.sql" {
		t.Fatalf("ReadFile called with path %q, want %q", src.readPath, "services/svc/models/table_e.sql")
	}

	// Final proposed SQL URI must point to the source artifact.
	const wantSuffix = "attempt-1.source.sql"
	if !strings.HasSuffix(p.ProposedSQLURI, wantSuffix) {
		t.Fatalf("ProposedSQLURI %q must end with %q", p.ProposedSQLURI, wantSuffix)
	}

	// Candidate URI must point to the candidate (Step-1) artifact.
	const wantCandSuffix = "attempt-1.sql"
	if !strings.HasSuffix(p.CandidateFixSQLURI, wantCandSuffix) {
		t.Fatalf("CandidateFixSQLURI %q must end with %q", p.CandidateFixSQLURI, wantCandSuffix)
	}

	// Exactly 4 artifacts: .sql, .diff, .source.sql, .source.diff.
	if len(art.written) != 4 {
		t.Fatalf("expected 4 artifacts, got %d: %v", len(art.written), art.written)
	}
	for _, key := range []string{
		"proposed-fix/r1/s.n/attempt-1.sql",
		"proposed-fix/r1/s.n/attempt-1.diff",
		"proposed-fix/r1/s.n/attempt-1.source.sql",
		"proposed-fix/r1/s.n/attempt-1.source.diff",
	} {
		if _, ok := art.written[key]; !ok {
			t.Fatalf("missing artifact key %q; got %v", key, art.written)
		}
	}

	// The outbox event must carry SourceResolved=true and the source URI.
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox entry, got %d", len(u.ob.entries))
	}
	var ev2 event.RemediationProposed
	_ = json.Unmarshal(u.ob.entries[0].Payload, &ev2)
	if !ev2.SourceResolved {
		t.Fatal("outbox event SourceResolved must be true")
	}
	if !strings.HasSuffix(ev2.ProposedSQLURI, "attempt-1.source.sql") {
		t.Fatalf("outbox ProposedSQLURI %q must end with attempt-1.source.sql", ev2.ProposedSQLURI)
	}
}

// TestProposeFix_Step2_FallbackOnSourceError verifies that when the source
// reader returns an error, the handler falls back to the candidate proposal:
// SourceResolved=false, ProposedSQLURI points to the candidate, 2 artifacts
// written, and the proposal is still status=proposed with one outbox emit.
func TestProposeFix_Step2_FallbackOnSourceError(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}

	d := Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              &llm,
		Evidence:         ev,
		Ancestry:         fakeAncestry{fp: "models/table_e.sql", svc: "svc"},
		Source:           &fakeSource{err: fmt.Errorf("github 503")},
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}

	if err := ProposeFix(context.Background(), d, baseTrigger()); err != nil {
		t.Fatal(err)
	}

	if len(u.pr.inserted) != 1 {
		t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
	}
	p := u.pr.inserted[0]

	if p.Status != proposal.StatusProposed {
		t.Fatalf("expected proposed status, got %s", p.Status)
	}
	if p.SourceResolved {
		t.Fatal("expected SourceResolved=false on source read error")
	}
	if !strings.HasSuffix(p.ProposedSQLURI, "attempt-1.sql") {
		t.Fatalf("ProposedSQLURI %q must end with attempt-1.sql (candidate fallback)", p.ProposedSQLURI)
	}

	// Only candidate artifacts written (no source step artifacts).
	if len(art.written) != 2 {
		t.Fatalf("expected 2 artifacts on fallback, got %d: %v", len(art.written), art.written)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox emit, got %d", len(u.ob.entries))
	}
}

// TestProposeFix_Step2_FallbackOnEmptyFilePath verifies that when ancestry
// returns an empty file path, the handler skips the source read entirely and
// falls back to the candidate proposal with SourceResolved=false.
func TestProposeFix_Step2_FallbackOnEmptyFilePath(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	// sourceReadCount is implicitly checked: fakeSource with err would cause a
	// test failure if ReadFile were called, but it won't be because filePath=="".
	art := &fakeArtifacts{}

	src := &fakeSource{err: fmt.Errorf("must not be called")}
	d := Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              &llm,
		Evidence:         ev,
		Ancestry:         fakeAncestry{fp: "", svc: "svc"}, // empty file path → skip Step 2.
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}

	if err := ProposeFix(context.Background(), d, baseTrigger()); err != nil {
		t.Fatal(err)
	}

	p := u.pr.inserted[0]
	if p.SourceResolved {
		t.Fatal("expected SourceResolved=false when file path is empty")
	}
	if !strings.HasSuffix(p.ProposedSQLURI, "attempt-1.sql") {
		t.Fatalf("ProposedSQLURI %q must be the candidate URI when file path empty", p.ProposedSQLURI)
	}
	if len(art.written) != 2 {
		t.Fatalf("expected 2 artifacts (candidate only) when file path empty, got %d", len(art.written))
	}
	// ReadFile must not have been called if filePath is empty.
	if src.readPath != "" {
		t.Fatalf("ReadFile must not be called when filePath is empty, but was called with %q", src.readPath)
	}
}

// TestProposeFix_Step2_FallbackOnUnmappedService verifies that when the
// node's service name has no entry in ServiceRepoPaths, Step 2 degrades:
// SourceResolved=false and ReadFile is never called.
func TestProposeFix_Step2_FallbackOnUnmappedService(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}, nil)
	art := &fakeArtifacts{}
	src := &fakeSource{content: "should not be called"}

	d := Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              &llm,
		Evidence:         ev,
		Ancestry:         fakeAncestry{fp: "models/table_e.sql", svc: "unknown-service"},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		Bucket:           "bucket",
		ServiceRepoPaths: map[string]string{"svc": "services/svc"}, // "unknown-service" not present
	}

	if err := ProposeFix(context.Background(), d, baseTrigger()); err != nil {
		t.Fatal(err)
	}

	p := u.pr.inserted[0]
	if p.SourceResolved {
		t.Fatal("expected SourceResolved=false when service name not in ServiceRepoPaths")
	}
	if !strings.HasSuffix(p.ProposedSQLURI, "attempt-1.sql") {
		t.Fatalf("ProposedSQLURI %q must be the candidate URI on unmapped service", p.ProposedSQLURI)
	}
	if len(art.written) != 2 {
		t.Fatalf("expected 2 artifacts (candidate only), got %d: %v", len(art.written), art.written)
	}
	// ReadFile must not have been called.
	if src.readPath != "" {
		t.Fatalf("ReadFile must not be called for unmapped service, but was called with %q", src.readPath)
	}
}

// TestProposeFix_SourceResolved_PersistsSourceLocation verifies that when Step
// 2 succeeds, the inserted proposal carries the three source-location fields:
// Repo, CommitSHA, and FilePath (the full repo-relative path passed to
// ReadFile). On the candidate-fallback path these fields must remain empty.
func TestProposeFix_SourceResolved_PersistsSourceLocation(t *testing.T) {
	t.Run("source_resolved_fields_populated", func(t *testing.T) {
		u := newFakeUoW()
		ev := fakeEvidence{vals: map[string]string{
			"s3://b/sql": "select custmer_id from t",
			"s3://b/log": "column does not exist",
		}}
		// Step-1 returns the candidate fix; Step-2 returns the corrected source.
		llm := fakeLLM{
			queue: []ports.ProposeResult{
				{ProposedSQL: "select customer_id from t", Rationale: "typo fix", Confidence: "high", Model: "m"},
				{ProposedSQL: "{{ config() }}\nselect customer_id from {{ ref('t') }}", Rationale: "typo fix", Confidence: "high", Model: "m"},
			},
			errs: []error{nil, nil},
		}
		art := &fakeArtifacts{}
		src := &fakeSource{content: "{{ config() }}\nselect custmer_id from {{ ref('t') }}"}
		d := Deps{
			NewUoW:           func() uow.UnitOfWork { return u },
			LLM:              &llm,
			Evidence:         ev,
			Ancestry:         fakeAncestry{fp: "models/orders_d.sql", svc: "service-3", a: []prompt.Ancestor{}},
			Source:           src,
			Sanitizer:        fakeSanitizer{},
			Artifacts:        art,
			Clock:            fakeClock{},
			Logger:           slog.Default(),
			MaxAttempts:      3,
			Bucket:           "bucket",
			ServiceRepoPaths: map[string]string{"service-3": "services/service-3"},
		}
		tr := baseTrigger()
		tr.Repo = "owner/continuo-dbt-demo"
		tr.CommitSHA = "abc123"

		if err := ProposeFix(context.Background(), d, tr); err != nil {
			t.Fatal(err)
		}

		if len(u.pr.inserted) != 1 {
			t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
		}
		p := u.pr.inserted[0]

		if !p.SourceResolved {
			t.Fatal("expected SourceResolved=true")
		}
		if p.Repo != "owner/continuo-dbt-demo" {
			t.Errorf("Repo = %q, want %q", p.Repo, "owner/continuo-dbt-demo")
		}
		if p.CommitSHA != "abc123" {
			t.Errorf("CommitSHA = %q, want %q", p.CommitSHA, "abc123")
		}
		if p.FilePath != "services/service-3/models/orders_d.sql" {
			t.Errorf("FilePath = %q, want %q", p.FilePath, "services/service-3/models/orders_d.sql")
		}
	})

	t.Run("candidate_fallback_fields_empty", func(t *testing.T) {
		u := newFakeUoW()
		ev := fakeEvidence{vals: map[string]string{
			"s3://b/sql": "select custmer_id from t",
			"s3://b/log": "column does not exist",
		}}
		llm := newFakeLLM(ports.ProposeResult{
			ProposedSQL: "select customer_id from t",
			Rationale:   "typo",
			Confidence:  "high",
			Model:       "m",
		}, nil)
		art := &fakeArtifacts{}
		// Source read error forces candidate fallback.
		d := Deps{
			NewUoW:           func() uow.UnitOfWork { return u },
			LLM:              &llm,
			Evidence:         ev,
			Ancestry:         fakeAncestry{fp: "models/orders_d.sql", svc: "service-3"},
			Source:           &fakeSource{err: fmt.Errorf("github 503")},
			Sanitizer:        fakeSanitizer{},
			Artifacts:        art,
			Clock:            fakeClock{},
			Logger:           slog.Default(),
			MaxAttempts:      3,
			Bucket:           "bucket",
			ServiceRepoPaths: map[string]string{"service-3": "services/service-3"},
		}
		tr := baseTrigger()
		tr.Repo = "owner/continuo-dbt-demo"
		tr.CommitSHA = "abc123"

		if err := ProposeFix(context.Background(), d, tr); err != nil {
			t.Fatal(err)
		}

		if len(u.pr.inserted) != 1 {
			t.Fatalf("expected 1 proposal row, got %d", len(u.pr.inserted))
		}
		p := u.pr.inserted[0]

		if p.SourceResolved {
			t.Fatal("expected SourceResolved=false on candidate fallback path")
		}
		if p.Repo != "" {
			t.Errorf("Repo must be empty on fallback path, got %q", p.Repo)
		}
		if p.CommitSHA != "" {
			t.Errorf("CommitSHA must be empty on fallback path, got %q", p.CommitSHA)
		}
		if p.FilePath != "" {
			t.Errorf("FilePath must be empty on fallback path, got %q", p.FilePath)
		}
	})
}

// TestProposeFix_Step2_FallbackOnUnchangedOrLowConfidence verifies that when
// the Step-2 LLM returns the same SQL as the original source, or returns a
// low-confidence result, the handler degrades to the candidate proposal:
// SourceResolved=false, only 2 artifacts written.
func TestProposeFix_Step2_FallbackOnUnchangedOrLowConfidence(t *testing.T) {
	originalSQL := "{{ config(materialized='table') }}\nselect custmer_id from {{ ref('t') }}"

	t.Run("unchanged_sql", func(t *testing.T) {
		u := newFakeUoW()
		ev := fakeEvidence{vals: map[string]string{
			"s3://b/sql": "select custmer_id from t",
			"s3://b/log": "column does not exist",
		}}
		llm := fakeLLM{
			queue: []ports.ProposeResult{
				{ProposedSQL: "select customer_id from t", Rationale: "typo fix", Confidence: "high", Model: "m"},
				// Step-2 returns the same source unchanged — not a fix.
				{ProposedSQL: originalSQL, Rationale: "no change", Confidence: "high", Model: "m"},
			},
			errs: []error{nil, nil},
		}
		art := &fakeArtifacts{}
		d := Deps{
			NewUoW:           func() uow.UnitOfWork { return u },
			LLM:              &llm,
			Evidence:         ev,
			Ancestry:         fakeAncestry{fp: "models/table_e.sql", svc: "svc", a: []prompt.Ancestor{}},
			Source:           &fakeSource{content: originalSQL},
			Sanitizer:        fakeSanitizer{},
			Artifacts:        art,
			Clock:            fakeClock{},
			Logger:           slog.Default(),
			MaxAttempts:      3,
			Bucket:           "bucket",
			ServiceRepoPaths: map[string]string{"svc": "services/svc"},
		}
		if err := ProposeFix(context.Background(), d, baseTrigger()); err != nil {
			t.Fatal(err)
		}
		p := u.pr.inserted[0]
		if p.SourceResolved {
			t.Fatal("expected SourceResolved=false when Step-2 SQL equals original")
		}
		if len(art.written) != 2 {
			t.Fatalf("expected 2 candidate-only artifacts, got %d: %v", len(art.written), art.written)
		}
	})

	t.Run("low_confidence", func(t *testing.T) {
		u := newFakeUoW()
		ev := fakeEvidence{vals: map[string]string{
			"s3://b/sql": "select custmer_id from t",
			"s3://b/log": "column does not exist",
		}}
		llm := fakeLLM{
			queue: []ports.ProposeResult{
				{ProposedSQL: "select customer_id from t", Rationale: "typo fix", Confidence: "high", Model: "m"},
				// Step-2 returns a different SQL but with low confidence.
				{ProposedSQL: "select fixed_id from t", Rationale: "maybe", Confidence: "low", Model: "m"},
			},
			errs: []error{nil, nil},
		}
		art := &fakeArtifacts{}
		d := Deps{
			NewUoW:           func() uow.UnitOfWork { return u },
			LLM:              &llm,
			Evidence:         ev,
			Ancestry:         fakeAncestry{fp: "models/table_e.sql", svc: "svc", a: []prompt.Ancestor{}},
			Source:           &fakeSource{content: originalSQL},
			Sanitizer:        fakeSanitizer{},
			Artifacts:        art,
			Clock:            fakeClock{},
			Logger:           slog.Default(),
			MaxAttempts:      3,
			Bucket:           "bucket",
			ServiceRepoPaths: map[string]string{"svc": "services/svc"},
		}
		if err := ProposeFix(context.Background(), d, baseTrigger()); err != nil {
			t.Fatal(err)
		}
		p := u.pr.inserted[0]
		if p.SourceResolved {
			t.Fatal("expected SourceResolved=false when Step-2 confidence is low")
		}
		if len(art.written) != 2 {
			t.Fatalf("expected 2 candidate-only artifacts, got %d: %v", len(art.written), art.written)
		}
	})
}
