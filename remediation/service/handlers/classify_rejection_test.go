package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/event"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/domain/repository"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
)

// --- fakes ---

type fakeLogReader struct {
	text string
	err  error
	// calls, when set, is incremented on every Fetch — so a test can assert the
	// reader was never called (e.g. a duplicate_table classification, which has
	// no log to read).
	calls *int
}

func (f fakeLogReader) Fetch(_ context.Context, _ string) (string, error) {
	if f.calls != nil {
		*f.calls++
	}
	return f.text, f.err
}

// mapLogReader returns a different body per URI, so a test can give the dbt log
// and the run-results artifact distinct contents.
type mapLogReader struct{ byURI map[string]string }

func (m mapLogReader) Fetch(_ context.Context, uri string) (string, error) {
	if v, ok := m.byURI[uri]; ok {
		return v, nil
	}
	return "", ports.ErrLogNotFound
}

type fakeClock struct{}

func (fakeClock) Now() time.Time { return time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC) }

type fakeDecisionRepo struct {
	saved    []repository.ClassificationDecision
	inserted bool
	// insertedByNode, when set, overrides inserted per node id.
	insertedByNode map[string]bool
}

func (r *fakeDecisionRepo) Upsert(_ context.Context, d repository.ClassificationDecision) (bool, error) {
	r.saved = append(r.saved, d)
	if r.insertedByNode != nil {
		return r.insertedByNode[d.NodeID], nil
	}
	return r.inserted, nil
}

// fakeOutbox implements outbox.Repository for in-process tests.
type fakeOutbox struct{ entries []*outbox.Entry }

func (o *fakeOutbox) Create(_ context.Context, e *outbox.Entry) error {
	o.entries = append(o.entries, e)
	return nil
}

func (o *fakeOutbox) GetPendingBatch(_ context.Context, _ int) ([]*outbox.Entry, error) {
	return nil, nil
}

func (o *fakeOutbox) MarkProcessed(_ context.Context, _ uuid.UUID) error        { return nil }
func (o *fakeOutbox) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }
func (o *fakeOutbox) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (o *fakeOutbox) IncrementRetry(_ context.Context, _ uuid.UUID) error       { return nil }

type fakeUoW struct {
	dec       *fakeDecisionRepo
	ob        *fakeOutbox
	committed bool
}

func (u *fakeUoW) Begin(context.Context) error                               { return nil }
func (u *fakeUoW) Commit() error                                             { u.committed = true; return nil }
func (u *fakeUoW) Rollback() error                                           { return nil }
func (u *fakeUoW) DecisionRepo() repository.ClassificationDecisionRepository { return u.dec }
func (u *fakeUoW) OutboxRepo() outbox.Repository                             { return u.ob }

func depsWith(u *fakeUoW, log string, err error) Deps {
	return Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: fakeLogReader{text: log, err: err},
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
}

// --- tests ---

func TestClassifyFailure_LogicEmitsTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:        failure.SourceValidation,
		ReleaseID:     "r1",
		NodeID:        "s.n",
		Repo:          "o/r",
		CommitSHA:     "abc",
		DBTLogURI:     "s3://b/k",
		CodeBundleURI: "s3://b/code-bundles/rel-1/bundle.json",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryLogic {
		t.Fatalf("decision not recorded as logic: %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if len(p.Nodes) != 1 {
		t.Fatalf("expected 1 node in batched trigger, got %d", len(p.Nodes))
	}
	n := p.Nodes[0]
	if n.Category != "logic" || n.NodeID != "s.n" || p.EventID != event.RemediationEventID("r1", 1).String() {
		t.Fatalf("bad trigger payload: %+v", p)
	}
	if n.Reason != "logic:missing_object" {
		t.Fatalf("trigger reason = %q, want logic:missing_object", n.Reason)
	}
	if n.ErrorExcerpt == "" {
		t.Fatal("trigger error_excerpt must not be empty")
	}
	if p.CodeBundleURI != "s3://b/code-bundles/rel-1/bundle.json" {
		t.Fatalf("trigger code_bundle_uri = %q, want s3://b/code-bundles/rel-1/bundle.json", p.CodeBundleURI)
	}
	if !u.committed {
		t.Fatal("tx not committed")
	}
}

func TestClassifyFailure_EmitsHealableNodes(t *testing.T) {
	// A healable failure classifies as emit, and the release's rejection
	// enqueues exactly one trigger for it.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:    failure.SourceValidation,
		ReleaseID: "r1",
		NodeID:    "s.n",
		DBTLogURI: "s3://b/k",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 {
		t.Fatalf("expected 1 decision recorded, got %d", len(u.dec.saved))
	}
	d := u.dec.saved[0]
	if d.Decision != failure.DecisionEmit {
		t.Fatalf("decision = %q, want emit", d.Decision)
	}
	if d.Reason != "logic:missing_object" {
		t.Fatalf("reason = %q, want logic:missing_object", d.Reason)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("healable failure must emit exactly 1 trigger, got %d", len(u.ob.entries))
	}
}

func TestClassifyFailure_StructuredBranchWinsOverLog(t *testing.T) {
	// The text log alone would classify infra (drop, no trigger); the structured
	// run_results says status=fail (a test), which is healable and must emit.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:        failure.SourceValidation,
		ReleaseID:     "r1",
		NodeID:        "s.n",
		DBTLogURI:     "s3://b/log",
		RunResultsURI: "run-results/x.json",
	}
	reader := mapLogReader{byURI: map[string]string{
		"s3://b/log":         "could not connect to database: connection refused",
		"run-results/x.json": `{"schema_version":1,"status":"fail","message":"Failure in test not_null_x","failures":3,"unique_id":"test.svc.x"}`,
	}}
	deps := Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: reader,
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
	if err := ClassifyRejection(context.Background(), deps, []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryTest {
		t.Fatalf("expected structured test classification, got %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("test category is healable → expected 1 trigger, got %d", len(u.ob.entries))
	}
}

// TestClassifyFailure_MalformedRunResultsFallsBackToLog confirms the
// fail-safe path when run_results does not decode as the structured
// contract (missing schema_version here — the same shape that used to
// satisfy the old lenient parser but does not satisfy the strict one): the
// parse error is logged and swallowed, structured stays nil, and
// classification falls back to the text log rather than using the malformed
// body's fields or failing the whole classification.
func TestClassifyFailure_MalformedRunResultsFallsBackToLog(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:        failure.SourceValidation,
		ReleaseID:     "r1",
		NodeID:        "s.n",
		DBTLogURI:     "s3://b/log",
		RunResultsURI: "run-results/x.json",
	}
	reader := mapLogReader{byURI: map[string]string{
		"s3://b/log":         "could not connect to database: connection refused",
		"run-results/x.json": `{"status":"error","message":"decoy: not a real contract result"}`,
	}}
	deps := Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: reader,
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
	if err := ClassifyRejection(context.Background(), deps, []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryInfraTransient {
		t.Fatalf("expected text-log fallback classification (infra_transient), got %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 0 {
		t.Fatalf("infra_transient is dropped, not healable → expected 0 triggers, got %d", len(u.ob.entries))
	}
}

func TestClassifyFailure_InfraDropsNoTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: failure.SourceValidation, ReleaseID: "r1", NodeID: "s.n"}
	err := ClassifyRejection(context.Background(), depsWith(u, "could not connect to database: connection refused", nil), []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Decision != failure.DecisionDrop {
		t.Fatalf("infra failure must be recorded as drop: %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 0 {
		t.Fatalf("dropped failure must not emit a trigger, got %d", len(u.ob.entries))
	}
}

func TestClassifyFailure_IdempotentSkipsTrigger(t *testing.T) {
	// inserted=false → already classified in this round → no duplicate
	// trigger. The natural key that makes this a no-op is now scoped to
	// (source, release_id, remediation_round, node_id): a redelivery of the
	// same round's rejection is a no-op, but a later round for the same node
	// is a fresh insert (see TestClassifyFailure_DifferentRoundsBothInsertAndEmit).
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: false}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: failure.SourceValidation, ReleaseID: "r1", NodeID: "s.n", RemediationRound: 1}
	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.ob.entries) != 0 {
		t.Fatalf("redelivery must not re-emit, got %d", len(u.ob.entries))
	}
}

// TestClassifyFailure_DifferentRoundsBothInsertAndEmit verifies that a
// remediation round is a human's "try again" on the same rejected release,
// not a redelivery of the same message: reclassifying the same
// (source, release, node) at round 1 and then round 2 must record two
// decisions and enqueue two triggers, and the triggers must be
// distinguishable — different remediation_round, different event_id — so the
// agent never conflates a retry's proposal with the original attempt's.
func TestClassifyFailure_DifferentRoundsBothInsertAndEmit(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	deps := depsWith(u, `Database Error: column "x" does not exist`, nil)
	ev := failure.FailureEvidence{
		Source:    failure.SourceValidation,
		ReleaseID: "r1",
		NodeID:    "s.n",
		DBTLogURI: "s3://b/k",
	}

	ev.RemediationRound = 1
	if err := ClassifyRejection(context.Background(), deps, []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}
	ev.RemediationRound = 2
	if err := ClassifyRejection(context.Background(), deps, []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}

	if len(u.dec.saved) != 2 {
		t.Fatalf("expected 2 decisions recorded (one per round), got %d", len(u.dec.saved))
	}
	if u.dec.saved[0].RemediationRound != 1 || u.dec.saved[1].RemediationRound != 2 {
		t.Fatalf("decision rounds = %d, %d; want 1, 2", u.dec.saved[0].RemediationRound, u.dec.saved[1].RemediationRound)
	}

	if len(u.ob.entries) != 2 {
		t.Fatalf("expected 2 outbox triggers (one per round), got %d", len(u.ob.entries))
	}
	var p1, p2 event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p1)
	_ = json.Unmarshal(u.ob.entries[1].Payload, &p2)
	if p1.RemediationRound != 1 {
		t.Fatalf("first trigger remediation_round = %d, want 1", p1.RemediationRound)
	}
	if p2.RemediationRound != 2 {
		t.Fatalf("second trigger remediation_round = %d, want 2", p2.RemediationRound)
	}
	if p1.EventID == p2.EventID {
		t.Fatal("triggers from different rounds must have different event_id")
	}
	if p1.EventID != event.RemediationEventID("r1", 1).String() {
		t.Fatalf("first trigger event_id = %q, want RemediationEventID(r1,1)", p1.EventID)
	}
	if p2.EventID != event.RemediationEventID("r1", 2).String() {
		t.Fatalf("second trigger event_id = %q, want RemediationEventID(r1,2)", p2.EventID)
	}
}

func TestClassifyFailure_SeedSourceFilePathIsEmpty(t *testing.T) {
	// A seed_build failure whose dbt log names a seeds/...csv path must NOT
	// have file_path populated in the RemediationRequested trigger. Seed
	// failures carry a real dbt node unique_id, so the remediation agent
	// resolves the source file via the orchestrator's GetNodeLocation.
	// Populating file_path for seeds would send a service-name-like NodeID
	// as serviceName in the agent, causing a miss on ServiceRepoPaths.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	logText := `Runtime Error in seed fx_rates_eur (seeds/customers.csv)
  Could not find file seeds/customers.csv`
	ev := failure.FailureEvidence{
		Source:    failure.SourceSeed,
		ReleaseID: "r3",
		NodeID:    "analytics.seed_fx_rates_eur",
		Repo:      "org/repo",
		CommitSHA: "abc123",
		DBTLogURI: "s3://bucket/seed.log",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, logText, nil), []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if len(p.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(p.Nodes))
	}
	n := p.Nodes[0]
	if n.FilePath != "" {
		t.Fatalf("seed trigger must have empty file_path so agent routes via ancestry; got %q", n.FilePath)
	}
	if n.NodeID != "analytics.seed_fx_rates_eur" {
		t.Fatalf("node_id must be preserved, got %q", n.NodeID)
	}
}

func TestClassifyFailure_CompileSourceThreadsFilePath(t *testing.T) {
	// A compile-stage failure with a Jinja syntax error must be classified as
	// logic (healable), emitted, and the file path extracted from the log must
	// appear in the RemediationRequested trigger payload.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	logText := "Compilation Error in model daily_transactions (models/daily_transactions.sql)\n  got '='"
	ev := failure.FailureEvidence{
		Source:    failure.SourceCompile,
		ReleaseID: "r2",
		NodeID:    "svc.daily_transactions",
		Repo:      "org/repo",
		CommitSHA: "def456",
		DBTLogURI: "s3://bucket/compile.log",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, logText, nil), []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	// Must be classified as healable logic.
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryLogic {
		t.Fatalf("compile syntax error must classify as logic: %+v", u.dec.saved)
	}
	// Must emit a trigger (healable).
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if len(p.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(p.Nodes))
	}
	n := p.Nodes[0]
	if n.FilePath != "models/daily_transactions.sql" {
		t.Fatalf("trigger payload file_path = %q, want %q", n.FilePath, "models/daily_transactions.sql")
	}
	if n.NodeID != "svc.daily_transactions" {
		t.Fatalf("trigger payload node_id = %q", n.NodeID)
	}
}

func TestClassifyFailure_DuplicateTableReadsNoLog(t *testing.T) {
	// The reader is wired to fail any Fetch and to count calls: a duplicate_table
	// rejection happens at parse time, before any Job runs, so classify() must
	// short-circuit before ever reaching the log reader.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	calls := 0
	deps := Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: fakeLogReader{err: errors.New("log reader must not be called for duplicate_table"), calls: &calls},
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
	// Both claimants are in the SAME service with different file paths — the
	// case OtherFilePath exists for, since OtherService alone cannot
	// discriminate the competing node here.
	ev := failure.FailureEvidence{
		Source:        failure.SourceDuplicateTable,
		ReleaseID:     "rel-1",
		NodeID:        "analytics.orders_v2",
		RelationID:    "analytics.orders",
		Service:       "marketing",
		FilePath:      "models/orders.sql",
		NodeType:      "dbt-model",
		OtherService:  "marketing",
		OtherFilePath: "models/orders_legacy.sql",
		Repo:          "owner/repo",
		CommitSHA:     "abc123",
	}
	err := ClassifyRejection(context.Background(), deps, []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("duplicate_table classification must not read the dbt log, reader was called %d time(s)", calls)
	}

	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryLogic {
		t.Fatalf("expected logic category, got %+v", u.dec.saved)
	}
	if u.dec.saved[0].Decision != failure.DecisionEmit {
		t.Fatalf("expected emit decision, got %+v", u.dec.saved[0])
	}

	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if p.Source != "duplicate_table" {
		t.Fatalf("trigger source = %q, want duplicate_table", p.Source)
	}
	if len(p.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(p.Nodes))
	}
	n := p.Nodes[0]
	if n.NodeID != "analytics.orders_v2" {
		t.Fatalf("trigger node_id = %q, want analytics.orders_v2", n.NodeID)
	}
	if n.RelationID != "analytics.orders" {
		t.Fatalf("trigger relation_id = %q, want analytics.orders", n.RelationID)
	}
	if n.Service != "marketing" {
		t.Fatalf("trigger service = %q, want marketing", n.Service)
	}
	if n.FilePath != "models/orders.sql" {
		t.Fatalf("trigger file_path = %q, want models/orders.sql", n.FilePath)
	}
	if n.NodeType != "dbt-model" {
		t.Fatalf("trigger node_type = %q, want dbt-model", n.NodeType)
	}
	if n.OtherService != "marketing" {
		t.Fatalf("trigger other_service = %q, want marketing", n.OtherService)
	}
	if n.OtherFilePath != "models/orders_legacy.sql" {
		t.Fatalf("trigger other_file_path = %q, want models/orders_legacy.sql", n.OtherFilePath)
	}
}

func TestClassifyRejection_TwoNodesEmitOneBatchedTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	base := failure.FailureEvidence{
		Source: failure.SourceValidation, ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc",
		DBTLogURI: "s3://b/k", CodeBundleURI: "s3://b/code-bundles/r1/bundle.json",
	}
	a, b := base, base
	anc := []failure.ChangedAncestor{{NodeID: "s.u", FilePath: "models/u.sql", Service: "svc"}}
	a.NodeID, a.ChangedAncestors = "s.a", anc
	b.NodeID, b.ChangedAncestors = "s.b", anc

	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), []failure.FailureEvidence{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 2 {
		t.Fatalf("both nodes must get a decision row, got %d", len(u.dec.saved))
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("one rejected release emits ONE trigger, got %d", len(u.ob.entries))
	}
	if u.ob.entries[0].StreamName != streams.RemediationRequestedV2 {
		t.Fatalf("stream = %q", u.ob.entries[0].StreamName)
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if p.EventID != event.RemediationEventID("r1", 1).String() || p.ReleaseID != "r1" || p.Source != "validation" {
		t.Fatalf("bad batch header: %+v", p)
	}
	if len(p.Nodes) != 2 || p.Nodes[0].NodeID != "s.a" || p.Nodes[1].NodeID != "s.b" {
		t.Fatalf("nodes must be carried in evidence order: %+v", p.Nodes)
	}
	if len(p.Nodes[0].ChangedAncestors) != 1 || p.Nodes[0].ChangedAncestors[0].NodeID != "s.u" ||
		p.Nodes[0].ChangedAncestors[0].FilePath != "models/u.sql" || p.Nodes[0].ChangedAncestors[0].Service != "svc" {
		t.Fatalf("changed ancestors must travel onto the trigger with their candidate location: %+v", p.Nodes[0])
	}
	if p.Nodes[0].ErrorSignature == "" || p.Nodes[0].ErrorSignature != p.Nodes[1].ErrorSignature {
		t.Fatalf("same error line must yield the same signature on both nodes: %+v", p.Nodes)
	}
}

func TestClassifyRejection_OnlyNewlyRecordedEmitNodesAreCarried(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{insertedByNode: map[string]bool{"s.a": false, "s.b": true}}, ob: &fakeOutbox{}}
	base := failure.FailureEvidence{Source: failure.SourceValidation, ReleaseID: "r1", DBTLogURI: "s3://b/k"}
	a, b := base, base
	a.NodeID, b.NodeID = "s.a", "s.b"

	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), []failure.FailureEvidence{a, b})
	if err != nil {
		t.Fatal(err)
	}
	var p event.RemediationRequested
	_ = json.Unmarshal(u.ob.entries[0].Payload, &p)
	if len(p.Nodes) != 1 || p.Nodes[0].NodeID != "s.b" {
		t.Fatalf("a node already recorded must not be re-emitted: %+v", p.Nodes)
	}
}

func TestClassifyRejection_NoEmitDecisionsWritesNoTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: false}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: failure.SourceValidation, ReleaseID: "r1", NodeID: "s.a", DBTLogURI: "s3://b/k"}
	if err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}
	if len(u.ob.entries) != 0 {
		t.Fatal("a redelivered rejection must not re-emit")
	}
	if !u.committed {
		t.Fatal("decisions are still committed")
	}
}
