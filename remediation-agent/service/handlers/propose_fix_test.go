package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
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

func (u *fakeUoW) Begin(context.Context) error                       { return nil }
func (u *fakeUoW) Commit() error                                     { u.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                   { return nil }
func (u *fakeUoW) ProposalRepo() repository.ProposalRepository       { return u.pr }
func (u *fakeUoW) OutboxRepo() outbox.Repository                     { return u.ob }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return u.mp }

func newFakeUoW() *fakeUoW {
	return &fakeUoW{
		pr: &fakeProposalRepo{count: 0},
		ob: &fakeOutbox{},
		mp: newFakeMsgProcRepo(),
	}
}

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
		MessageID:       "1-0",
	}
}

func TestProposeFix_HappyPath(t *testing.T) {
	u := newFakeUoW()
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
	u := newFakeUoW()
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
	u := newFakeUoW()
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
	llm := fakeLLM{res: ports.ProposeResult{
		ProposedSQL: "select customer_id from t",
		Rationale:   "typo",
		Confidence:  "high",
		Model:       "m",
	}}
	art := &fakeArtifacts{}
	d := deps(u, ev, llm, art)

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
