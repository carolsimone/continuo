package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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

// classifiedCall is one decision a fakeClassifierMetrics counted, with whether
// the unit of work had committed by then.
type classifiedCall struct {
	decision, rule string
	committed      bool
}

// fakeClassifierMetrics records every decision it is asked to count, and
// whether the unit of work it watches had committed when each was counted.
type fakeClassifierMetrics struct {
	u     *fakeUoW
	calls []classifiedCall
}

func (m *fakeClassifierMetrics) Classified(decision, rule string) {
	m.calls = append(m.calls, classifiedCall{decision: decision, rule: rule, committed: m.u.committed})
}

// failingCommitUoW is a unit of work whose commit fails.
type failingCommitUoW struct{ *fakeUoW }

func (failingCommitUoW) Commit() error { return errors.New("commit failed") }

func depsWith(u *fakeUoW, log string, err error) Deps {
	return Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: fakeLogReader{text: log, err: err},
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
}

// testTenant is the tenant of the message the tests classify.
const testTenant = "acme"

// triggerOf returns the payload of a trigger's outbox row, which the handler
// stores under the tenant of the message it classified. It fails the test when
// the row is not stored under testTenant.
func triggerOf(t *testing.T, e *outbox.Entry) pkgevents.RemediationRequested {
	t.Helper()
	tenantID, payload, err := outbox.UnwrapTenant(e.Payload)
	require.NoError(t, err, "the trigger row is stored under a tenant")
	require.Equal(t, testTenant, tenantID)
	var p pkgevents.RemediationRequested
	require.NoError(t, json.Unmarshal(payload, &p))
	return p
}

// rowIDOf is the outbox row id of the trigger of one release and round, which
// the publisher renders as the entry's event_id.
func rowIDOf(releaseID string, round int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(event.RemediationEventID(releaseID, round).String()))
}

// --- tests ---

func TestClassifyFailure_LogicEmitsTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:        pkg_model.RemediationSourceValidation,
		ReleaseID:     "r1",
		NodeID:        "s.n",
		Repo:          "o/r",
		CommitSHA:     "abc",
		DBTLogURI:     "s3://b/k",
		CodeBundleURI: "s3://b/code-bundles/rel-1/bundle.json",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), testTenant, []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryLogic {
		t.Fatalf("decision not recorded as logic: %+v", u.dec.saved)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	p := triggerOf(t, u.ob.entries[0])
	if len(p.Nodes) != 1 {
		t.Fatalf("expected 1 node in batched trigger, got %d", len(p.Nodes))
	}
	n := p.Nodes[0]
	if n.Category != "logic" || n.NodeID != "s.n" || u.ob.entries[0].ID != rowIDOf("r1", 1) {
		t.Fatalf("bad trigger payload: %+v", p)
	}
	if n.Unfixable != nil {
		t.Fatalf("a failure a fix can reach carries no unfixable marker: %+v", n.Unfixable)
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
		Source:    pkg_model.RemediationSourceValidation,
		ReleaseID: "r1",
		NodeID:    "s.n",
		DBTLogURI: "s3://b/k",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), testTenant, []failure.FailureEvidence{ev})
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
	// The text log alone would classify infra (drop, forwarded unfixable); the structured
	// run_results says status=fail (a test), which is healable and must emit.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source:        pkg_model.RemediationSourceValidation,
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
	if err := ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}); err != nil {
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
		Source:        pkg_model.RemediationSourceValidation,
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
	if err := ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Category != failure.CategoryInfraTransient {
		t.Fatalf("expected text-log fallback classification (infra_transient), got %+v", u.dec.saved)
	}
	// The infra verdict is forwarded unfixable: the malformed structured result
	// did not decide it, the text log did.
	require.Len(t, u.ob.entries, 1)
	p := triggerOf(t, u.ob.entries[0])
	require.Len(t, p.Nodes, 1)
	require.NotNil(t, p.Nodes[0].Unfixable)
	assert.Equal(t, string(pkg_model.HandoverReasonClassifierInfra), p.Nodes[0].Unfixable.Reason)
}

// TestClassifyFailure_InfraIsForwardedUnfixable holds that a failure the
// classifier judges an infrastructure error is recorded as a drop and still
// reaches the trigger, marked unfixable with the reason classifier_infra and the
// failure's key error line as its detail (the rule that matched stays in the
// node's reason), with every other field of a forwarded node.
func TestClassifyFailure_InfraIsForwardedUnfixable(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{
		Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.n",
		Repo: "o/r", CommitSHA: "abc", DBTLogURI: "s3://b/k", FilePath: "models/n.sql", Service: "svc", NodeType: "dbt-model",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, "could not connect to database: connection refused", nil), testTenant, []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 1 || u.dec.saved[0].Decision != failure.DecisionDrop {
		t.Fatalf("infra failure must still be recorded as drop: %+v", u.dec.saved)
	}
	require.Len(t, u.ob.entries, 1, "a dropped failure is forwarded, not withheld")
	assert.Equal(t, streams.RemediationRequestedV3, u.ob.entries[0].StreamName)
	p := triggerOf(t, u.ob.entries[0])
	require.Len(t, p.Nodes, 1)
	n := p.Nodes[0]
	assert.Equal(t, "s.n", n.NodeID)
	assert.Equal(t, string(failure.CategoryInfraTransient), n.Category)
	assert.Equal(t, "infra:connection_refused", n.Reason)
	assert.NotEmpty(t, n.ErrorSignature)
	assert.Equal(t, "s3://b/k", n.DBTLogURI)
	assert.Equal(t, "models/n.sql", n.FilePath)
	assert.Equal(t, "svc", n.Service)
	require.NotNil(t, n.Unfixable)
	assert.Equal(t, pkgevents.Unfixable{Reason: string(pkg_model.HandoverReasonClassifierInfra), Detail: "could not connect to database: connection refused"}, *n.Unfixable)
	assert.True(t, u.committed)
}

func TestClassifyFailure_IdempotentSkipsTrigger(t *testing.T) {
	// inserted=false → already classified in this round → no duplicate
	// trigger. The natural key that makes this a no-op is now scoped to
	// (source, release_id, remediation_round, node_id): a redelivery of the
	// same round's rejection is a no-op, but a later round for the same node
	// is a fresh insert (see TestClassifyFailure_DifferentRoundsBothInsertAndEmit).
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: false}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.n", RemediationRound: 1}
	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), testTenant, []failure.FailureEvidence{ev})
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
		Source:    pkg_model.RemediationSourceValidation,
		ReleaseID: "r1",
		NodeID:    "s.n",
		DBTLogURI: "s3://b/k",
	}

	ev.RemediationRound = 1
	if err := ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}); err != nil {
		t.Fatal(err)
	}
	ev.RemediationRound = 2
	if err := ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}); err != nil {
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
	p1, p2 := triggerOf(t, u.ob.entries[0]), triggerOf(t, u.ob.entries[1])
	if p1.RemediationRound != 1 {
		t.Fatalf("first trigger remediation_round = %d, want 1", p1.RemediationRound)
	}
	if p2.RemediationRound != 2 {
		t.Fatalf("second trigger remediation_round = %d, want 2", p2.RemediationRound)
	}
	if u.ob.entries[0].ID == u.ob.entries[1].ID {
		t.Fatal("triggers from different rounds must be different outbox rows")
	}
	if u.ob.entries[0].ID != rowIDOf("r1", 1) {
		t.Fatalf("first trigger row id = %s, want the row of RemediationEventID(r1,1)", u.ob.entries[0].ID)
	}
	if u.ob.entries[1].ID != rowIDOf("r1", 2) {
		t.Fatalf("second trigger row id = %s, want the row of RemediationEventID(r1,2)", u.ob.entries[1].ID)
	}
}

func TestClassifyFailure_SeedSourceFilePathIsEmpty(t *testing.T) {
	// A seed_build failure whose dbt log names a seeds/...csv path must NOT
	// have file_path populated in the RemediationRequested trigger. Seed
	// failures carry a real dbt node unique_id, so the remediation agent
	// resolves the source file via catalog's GetNodeLocation.
	// Populating file_path for seeds would send a service-name-like NodeID
	// as serviceName in the agent, causing a miss on ServiceRepoPaths.
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	logText := `Runtime Error in seed fx_rates_eur (seeds/customers.csv)
  Could not find file seeds/customers.csv`
	ev := failure.FailureEvidence{
		Source:    pkg_model.RemediationSourceSeedBuild,
		ReleaseID: "r3",
		NodeID:    "analytics.seed_fx_rates_eur",
		Repo:      "org/repo",
		CommitSHA: "abc123",
		DBTLogURI: "s3://bucket/seed.log",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, logText, nil), testTenant, []failure.FailureEvidence{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("expected 1 outbox trigger, got %d", len(u.ob.entries))
	}
	p := triggerOf(t, u.ob.entries[0])
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
		Source:    pkg_model.RemediationSourceCompile,
		ReleaseID: "r2",
		NodeID:    "svc.daily_transactions",
		Repo:      "org/repo",
		CommitSHA: "def456",
		DBTLogURI: "s3://bucket/compile.log",
	}
	err := ClassifyRejection(context.Background(), depsWith(u, logText, nil), testTenant, []failure.FailureEvidence{ev})
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
	p := triggerOf(t, u.ob.entries[0])
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
		Source:        pkg_model.RemediationSourceDuplicateTable,
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
	err := ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev})
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
	p := triggerOf(t, u.ob.entries[0])
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
		Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc",
		DBTLogURI: "s3://b/k", CodeBundleURI: "s3://b/code-bundles/r1/bundle.json",
	}
	a, b := base, base
	anc := []failure.ChangedAncestor{{NodeID: "s.u", FilePath: "models/u.sql", Service: "svc", Depth: 1}}
	a.NodeID, a.ChangedAncestors = "s.a", anc
	b.NodeID, b.ChangedAncestors = "s.b", anc

	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), testTenant, []failure.FailureEvidence{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(u.dec.saved) != 2 {
		t.Fatalf("both nodes must get a decision row, got %d", len(u.dec.saved))
	}
	if len(u.ob.entries) != 1 {
		t.Fatalf("one rejected release emits ONE trigger, got %d", len(u.ob.entries))
	}
	if u.ob.entries[0].StreamName != streams.RemediationRequestedV3 {
		t.Fatalf("stream = %q", u.ob.entries[0].StreamName)
	}
	p := triggerOf(t, u.ob.entries[0])
	if u.ob.entries[0].ID != rowIDOf("r1", 1) || p.ReleaseID != "r1" || p.Source != "validation" {
		t.Fatalf("bad batch header: %+v", p)
	}
	if len(p.Nodes) != 2 || p.Nodes[0].NodeID != "s.a" || p.Nodes[1].NodeID != "s.b" {
		t.Fatalf("nodes must be carried in evidence order: %+v", p.Nodes)
	}
	if len(p.Nodes[0].ChangedAncestors) != 1 || p.Nodes[0].ChangedAncestors[0].NodeID != "s.u" ||
		p.Nodes[0].ChangedAncestors[0].FilePath != "models/u.sql" || p.Nodes[0].ChangedAncestors[0].Service != "svc" ||
		p.Nodes[0].ChangedAncestors[0].Depth != 1 {
		t.Fatalf("changed ancestors must travel onto the trigger with their candidate location: %+v", p.Nodes[0])
	}
	if p.Nodes[0].ErrorSignature == "" || p.Nodes[0].ErrorSignature != p.Nodes[1].ErrorSignature {
		t.Fatalf("same error line must yield the same signature on both nodes: %+v", p.Nodes)
	}
}

func TestClassifyRejection_OnlyNewlyRecordedNodesAreCarried(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{insertedByNode: map[string]bool{"s.a": false, "s.b": true}}, ob: &fakeOutbox{}}
	base := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", DBTLogURI: "s3://b/k"}
	a, b := base, base
	a.NodeID, b.NodeID = "s.a", "s.b"

	err := ClassifyRejection(context.Background(), depsWith(u, `Database Error: column "x" does not exist`, nil), testTenant, []failure.FailureEvidence{a, b})
	if err != nil {
		t.Fatal(err)
	}
	p := triggerOf(t, u.ob.entries[0])
	if len(p.Nodes) != 1 || p.Nodes[0].NodeID != "s.b" {
		t.Fatalf("a node already recorded must not be re-emitted: %+v", p.Nodes)
	}
}

// TestClassifyRejection_MixedBatchCarriesFixableAndUnfixable holds that one
// trigger carries every newly recorded node of the rejection, in evidence
// order, the unfixable one marked and the others not. Each node reads its own
// log, so the infra verdict of one does not touch the others.
func TestClassifyRejection_MixedBatchCarriesFixableAndUnfixable(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	deps := Deps{
		NewUoW: func() uow.UnitOfWork { return u },
		LogReader: mapLogReader{byURI: map[string]string{
			"s3://b/a": `Database Error: column "x" does not exist`,
			"s3://b/b": "could not connect to database: connection refused",
			"s3://b/c": "Failure in test not_null_c (got 3 results, configured to fail if != 0)",
		}},
		Clock:  fakeClock{},
		Logger: slog.Default(),
	}
	base := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc"}
	a, b, c := base, base, base
	a.NodeID, a.DBTLogURI = "s.a", "s3://b/a"
	b.NodeID, b.DBTLogURI = "s.b", "s3://b/b"
	c.NodeID, c.DBTLogURI = "s.c", "s3://b/c"

	require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{a, b, c}))

	require.Len(t, u.ob.entries, 1, "one rejected release is ONE trigger")
	p := triggerOf(t, u.ob.entries[0])
	require.Len(t, p.Nodes, 3)
	assert.Equal(t, []string{"s.a", "s.b", "s.c"}, []string{p.Nodes[0].NodeID, p.Nodes[1].NodeID, p.Nodes[2].NodeID})
	assert.Nil(t, p.Nodes[0].Unfixable)
	require.NotNil(t, p.Nodes[1].Unfixable)
	assert.Equal(t, string(pkg_model.HandoverReasonClassifierInfra), p.Nodes[1].Unfixable.Reason)
	assert.Nil(t, p.Nodes[2].Unfixable)
	assert.Len(t, u.dec.saved, 3, "every node is recorded, emit and drop alike")
}

// TestClassifyRejection_AllUnfixableStillEmitsOneTrigger holds that a release
// whose every failure is unfixable still writes its trigger, so the release
// page names each node and why no fix was proposed.
func TestClassifyRejection_AllUnfixableStillEmitsOneTrigger(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	base := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc", DBTLogURI: "s3://b/k"}
	a, b := base, base
	a.NodeID, b.NodeID = "s.a", "s.b"

	require.NoError(t, ClassifyRejection(context.Background(), depsWith(u, "OOMKilled: container exceeded its memory limit", nil), testTenant, []failure.FailureEvidence{a, b}))

	require.Len(t, u.ob.entries, 1, "every node unfixable still writes ONE trigger")
	assert.Equal(t, rowIDOf("r1", 1), u.ob.entries[0].ID)
	p := triggerOf(t, u.ob.entries[0])
	require.Len(t, p.Nodes, 2)
	for _, n := range p.Nodes {
		require.NotNil(t, n.Unfixable, n.NodeID)
		assert.Equal(t, string(pkg_model.HandoverReasonClassifierInfra), n.Unfixable.Reason)
		assert.Equal(t, "OOMKilled: container exceeded its memory limit", n.Unfixable.Detail)
		assert.Equal(t, "infra:oomkilled", n.Reason, "the rule that matched stays on the node's reason")
	}
	for _, d := range u.dec.saved {
		assert.Equal(t, failure.DecisionDrop, d.Decision, "the decision row still says drop")
	}
	assert.True(t, u.committed)
}

// TestClassifyRejection_RedeliveryOfUnfixableNodesWritesNothing holds that a
// redelivered rejection whose nodes were all recorded before writes no trigger,
// whether the nodes were fixable or not.
func TestClassifyRejection_RedeliveryOfUnfixableNodesWritesNothing(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: false}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.a", DBTLogURI: "s3://b/k"}

	require.NoError(t, ClassifyRejection(context.Background(), depsWith(u, "could not connect to database: connection refused", nil), testTenant, []failure.FailureEvidence{ev}))

	assert.Empty(t, u.ob.entries, "a redelivered rejection must not re-emit")
	assert.True(t, u.committed, "the decisions are still committed")
}

// TestClassifyRejection_ParseNotHealableIsForwardedUnfixable holds that a
// parse failure of a kind no source change fixes is forwarded unfixable with the
// reason classifier_unhealable_parse and the parser's own text as its detail,
// and that the rule id stays on the node's reason.
func TestClassifyRejection_ParseNotHealableIsForwardedUnfixable(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	var fetches int
	deps := Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: fakeLogReader{err: errors.New("log reader must not be called"), calls: &fetches},
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
	ev := failure.FailureEvidence{
		Source: pkg_model.RemediationSourceParse, ReleaseID: "rel-1", NodeID: "a.b",
		ParseKind: pkg_model.ParseFailureKindInternal, Detail: "parser crashed",
		FilePath: "models/b.sql", Service: "s", Repo: "o/r", CommitSHA: "sha",
	}

	require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}))

	require.Equal(t, 0, fetches)
	require.Len(t, u.dec.saved, 1)
	assert.Equal(t, failure.DecisionDrop, u.dec.saved[0].Decision)
	require.Len(t, u.ob.entries, 1)
	p := triggerOf(t, u.ob.entries[0])
	assert.Equal(t, "parse", p.Source)
	require.Len(t, p.Nodes, 1)
	n := p.Nodes[0]
	assert.Equal(t, "a.b", n.NodeID)
	assert.Equal(t, "parse:not_healable", n.Reason)
	assert.Equal(t, "parser crashed", n.ErrorExcerpt)
	assert.Equal(t, "models/b.sql", n.FilePath)
	require.NotNil(t, n.Unfixable)
	assert.Equal(t, pkgevents.Unfixable{Reason: string(pkg_model.HandoverReasonClassifierUnhealableParse), Detail: "parser crashed"}, *n.Unfixable)
}

// TestClassifyRejection_UnfixableDetailFallsBackToTheRule holds that a dropped
// node whose classification has no key error line is still handed over with a
// message: the rule that matched.
func TestClassifyRejection_UnfixableDetailFallsBackToTheRule(t *testing.T) {
	for name, detail := range map[string]string{"no detail": "", "a blank detail": " \n\t "} {
		t.Run(name, func(t *testing.T) {
			u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
			deps := Deps{
				NewUoW:    func() uow.UnitOfWork { return u },
				LogReader: fakeLogReader{err: errors.New("log reader must not be called")},
				Clock:     fakeClock{},
				Logger:    slog.Default(),
			}
			ev := failure.FailureEvidence{
				Source: pkg_model.RemediationSourceParse, ReleaseID: "rel-1", NodeID: "a.b",
				ParseKind: pkg_model.ParseFailureKindInternal, Detail: detail,
			}

			require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}))

			require.Len(t, u.ob.entries, 1)
			n := triggerOf(t, u.ob.entries[0]).Nodes[0]
			require.NotNil(t, n.Unfixable)
			assert.Equal(t, "parse:not_healable", n.Unfixable.Detail)
		})
	}
}

// TestClassifyRejection_UnfixableDetailIsTheCappedErrorLine holds that an
// infrastructure failure's detail is its key error line, cut to the excerpt's
// cap, never the whole log.
func TestClassifyRejection_UnfixableDetailIsTheCappedErrorLine(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.n", DBTLogURI: "s3://b/k"}
	log := "12:00:01  Encountered an error:\nconnection refused " + strings.Repeat("x", 10_000) + "\n12:00:02  next line"

	require.NoError(t, ClassifyRejection(context.Background(), depsWith(u, log, nil), testTenant, []failure.FailureEvidence{ev}))

	n := triggerOf(t, u.ob.entries[0]).Nodes[0]
	require.NotNil(t, n.Unfixable)
	assert.Equal(t, n.ErrorExcerpt, n.Unfixable.Detail, "the detail is the excerpt the trigger carries")
	assert.True(t, strings.HasPrefix(n.Unfixable.Detail, "connection refused xxx"), n.Unfixable.Detail[:40])
	assert.LessOrEqual(t, len(n.Unfixable.Detail), 4*1024)
	assert.NotContains(t, n.Unfixable.Detail, "next line")
}

// TestClassifyRejection_UnfixableDetailIsCleaned holds that the detail a
// dropped node's trigger entry carries has its credentials and email addresses
// replaced by fixed markers, whichever evidence the classification came from,
// while the excerpt kept for the case base is the raw line.
func TestClassifyRejection_UnfixableDetailIsCleaned(t *testing.T) {
	const line = "could not connect to database: connection refused postgres://u:hunter2@db/x, ask ops@acme.com"
	const want = "could not connect to database: connection refused postgres://u:[redacted:secret]@db/x, ask [redacted:email]"
	for name, ev := range map[string]failure.FailureEvidence{
		"a dbt log": {Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.n", DBTLogURI: "s3://b/k"},
		"a parse failure": {
			Source: pkg_model.RemediationSourceParse, ReleaseID: "r1", NodeID: "s.n",
			ParseKind: pkg_model.ParseFailureKindInternal, Detail: line,
		},
	} {
		t.Run(name, func(t *testing.T) {
			u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}

			require.NoError(t, ClassifyRejection(context.Background(), depsWith(u, line, nil), testTenant, []failure.FailureEvidence{ev}))

			require.Len(t, u.ob.entries, 1)
			n := triggerOf(t, u.ob.entries[0]).Nodes[0]
			require.NotNil(t, n.Unfixable)
			assert.Equal(t, want, n.Unfixable.Detail)
			assert.Equal(t, line, n.ErrorExcerpt, "the excerpt is the raw line")
		})
	}
}

// TestClassifyRejection_StoresTheTriggerUnderTheMessagesTenant holds that the
// trigger's outbox row carries the tenant of the message the rejection came
// from, and that a message with no tenant is refused before any row is written.
func TestClassifyRejection_StoresTheTriggerUnderTheMessagesTenant(t *testing.T) {
	ev := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.a", DBTLogURI: "s3://b/k"}

	t.Run("the row is stored under that tenant", func(t *testing.T) {
		u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
		require.NoError(t, ClassifyRejection(context.Background(), depsWith(u, `column "x" does not exist`, nil), "globex", []failure.FailureEvidence{ev}))
		require.Len(t, u.ob.entries, 1)
		tenantID, payload, err := outbox.UnwrapTenant(u.ob.entries[0].Payload)
		require.NoError(t, err)
		assert.Equal(t, "globex", tenantID)
		var p pkgevents.RemediationRequested
		require.NoError(t, json.Unmarshal(payload, &p), "the wrapped payload is the typed trigger")
		assert.Equal(t, "r1", p.ReleaseID)
	})

	t.Run("no tenant is refused", func(t *testing.T) {
		u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
		err := ClassifyRejection(context.Background(), depsWith(u, `column "x" does not exist`, nil), "", []failure.FailureEvidence{ev})
		require.ErrorIs(t, err, outbox.ErrNoTenant)
		assert.Empty(t, u.ob.entries)
		assert.False(t, u.committed, "a refused message commits nothing")
	})
}

// TestClassifyRejection_NoEvidenceRecordsAndEmitsNothing pins what a dropped
// rejection (no failed node left to fix) reaches: no unit of work, no recorded
// decision, no remediation.requested.
func TestClassifyRejection_NoEvidenceRecordsAndEmitsNothing(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{}, ob: &fakeOutbox{}}
	deps := depsWith(u, "", nil)
	begun := false
	deps.NewUoW = func() uow.UnitOfWork { begun = true; return u }

	require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, nil))

	assert.False(t, begun, "a rejection with no evidence opens no transaction")
	assert.Empty(t, u.dec.saved, "no decision is recorded")
	assert.Empty(t, u.ob.entries, "no remediation.requested is enqueued")
	assert.False(t, u.committed)
}

func TestClassifyRejection_ParseEvidenceReadsNoLog(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	var fetches int
	deps := Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		LogReader: fakeLogReader{err: errors.New("log reader must not be called"), calls: &fetches},
		Clock:     fakeClock{},
		Logger:    slog.Default(),
	}
	ev := failure.FailureEvidence{
		Source: pkg_model.RemediationSourceParse, ReleaseID: "rel-1", NodeID: "a.b",
		ParseKind: pkg_model.ParseFailureKindInvalidSQL, Detail: "Expecting )",
		FilePath: "models/b.sql", Service: "s", Repo: "o/r", CommitSHA: "sha",
	}
	require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev}))
	require.Equal(t, 0, fetches, "a parse failure has no log to fetch")
	require.Len(t, u.ob.entries, 1, "one trigger must be emitted")
	require.Equal(t, pkg_model.RemediationSourceParse, u.dec.saved[0].Source)
	w := triggerOf(t, u.ob.entries[0])
	assert.Equal(t, "parse", w.Source)
	require.Len(t, w.Nodes, 1)
	assert.Equal(t, "logic:invalid_sql", w.Nodes[0].Reason)
	assert.Equal(t, "Expecting )", w.Nodes[0].ErrorExcerpt)
	assert.Equal(t, "models/b.sql", w.Nodes[0].FilePath)
	assert.Nil(t, w.Nodes[0].Unfixable)
}

// TestClassifyRejection_CountsEachNewDecisionOnceCommitted holds that every
// newly recorded decision is counted with its decision and its rule, emit and
// drop alike, and only after the transaction that records it has committed.
func TestClassifyRejection_CountsEachNewDecisionOnceCommitted(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	m := &fakeClassifierMetrics{u: u}
	deps := Deps{
		NewUoW: func() uow.UnitOfWork { return u },
		LogReader: mapLogReader{byURI: map[string]string{
			"s3://b/a": `Database Error: column "x" does not exist`,
			"s3://b/b": "could not connect to database: connection refused",
			"s3://b/c": "some entirely novel failure mode",
		}},
		Clock:   fakeClock{},
		Logger:  slog.Default(),
		Metrics: m,
	}
	base := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc"}
	a, b, c := base, base, base
	a.NodeID, a.DBTLogURI = "s.a", "s3://b/a"
	b.NodeID, b.DBTLogURI = "s.b", "s3://b/b"
	c.NodeID, c.DBTLogURI = "s.c", "s3://b/c"

	require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{a, b, c}))

	assert.Equal(t, []classifiedCall{
		{decision: "emit", rule: "logic:missing_object", committed: true},
		{decision: "drop", rule: "infra:connection_refused", committed: true},
		{decision: "emit", rule: "unknown:unmatched", committed: true},
	}, m.calls)
}

// TestClassifyRejection_CountsAClassifierRuleOfEverySource holds that the
// decisions of a duplicate-relation rejection and of a parse rejection, healable
// or not, are counted too, with the rules their classifiers record.
func TestClassifyRejection_CountsAClassifierRuleOfEverySource(t *testing.T) {
	cases := map[string]struct {
		ev       failure.FailureEvidence
		decision string
		rule     string
	}{
		"duplicate table": {
			ev:       failure.FailureEvidence{Source: pkg_model.RemediationSourceDuplicateTable, ReleaseID: "r1", NodeID: "a.b", RelationID: "a.b"},
			decision: "emit", rule: "logic:duplicate_table",
		},
		"healable parse": {
			ev:       failure.FailureEvidence{Source: pkg_model.RemediationSourceParse, ReleaseID: "r1", NodeID: "a.b", ParseKind: pkg_model.ParseFailureKindInvalidSQL},
			decision: "emit", rule: "logic:invalid_sql",
		},
		"unhealable parse": {
			ev:       failure.FailureEvidence{Source: pkg_model.RemediationSourceParse, ReleaseID: "r1", NodeID: "a.b", ParseKind: pkg_model.ParseFailureKindInternal},
			decision: "drop", rule: "parse:not_healable",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
			m := &fakeClassifierMetrics{u: u}
			deps := depsWith(u, "", nil)
			deps.Metrics = m

			require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{tc.ev}))

			assert.Equal(t, []classifiedCall{{decision: tc.decision, rule: tc.rule, committed: true}}, m.calls)
		})
	}
}

// TestClassifyRejection_ARedeliveryCountsNothing holds that a rejection whose
// nodes were all recorded before counts no decision, and that a rejection with
// one new node among recorded ones counts only the new node.
func TestClassifyRejection_ARedeliveryCountsNothing(t *testing.T) {
	log := `Database Error: column "x" does not exist`
	base := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc", DBTLogURI: "s3://b/k"}
	a, b := base, base
	a.NodeID, b.NodeID = "s.a", "s.b"

	t.Run("every node recorded before", func(t *testing.T) {
		u := &fakeUoW{dec: &fakeDecisionRepo{inserted: false}, ob: &fakeOutbox{}}
		m := &fakeClassifierMetrics{u: u}
		deps := depsWith(u, log, nil)
		deps.Metrics = m

		require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{a, b}))

		assert.Empty(t, m.calls, "a redelivered rejection records nothing, so it counts nothing")
		assert.True(t, u.committed)
	})

	t.Run("one node new", func(t *testing.T) {
		u := &fakeUoW{dec: &fakeDecisionRepo{insertedByNode: map[string]bool{"s.a": false, "s.b": true}}, ob: &fakeOutbox{}}
		m := &fakeClassifierMetrics{u: u}
		deps := depsWith(u, log, nil)
		deps.Metrics = m

		require.NoError(t, ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{a, b}))

		assert.Equal(t, []classifiedCall{{decision: "emit", rule: "logic:missing_object", committed: true}}, m.calls)
	})
}

// TestClassifyRejection_ARolledBackTransactionCountsNothing holds that a
// decision whose transaction fails to commit is not counted, so the redelivery
// that records it counts it once.
func TestClassifyRejection_ARolledBackTransactionCountsNothing(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	m := &fakeClassifierMetrics{u: u}
	deps := depsWith(u, `Database Error: column "x" does not exist`, nil)
	deps.NewUoW = func() uow.UnitOfWork { return failingCommitUoW{u} }
	deps.Metrics = m
	ev := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.a", DBTLogURI: "s3://b/k"}

	err := ClassifyRejection(context.Background(), deps, testTenant, []failure.FailureEvidence{ev})

	require.Error(t, err)
	assert.Empty(t, m.calls)
}

// TestClassifyRejection_NoMetricsCountsNothingAndDoesNotFail holds that a Deps
// with no metrics wired still classifies.
func TestClassifyRejection_NoMetricsCountsNothingAndDoesNotFail(t *testing.T) {
	u := &fakeUoW{dec: &fakeDecisionRepo{inserted: true}, ob: &fakeOutbox{}}
	ev := failure.FailureEvidence{Source: pkg_model.RemediationSourceValidation, ReleaseID: "r1", NodeID: "s.a", DBTLogURI: "s3://b/k"}

	require.NoError(t, ClassifyRejection(context.Background(), depsWith(u, "x does not exist", nil), testTenant, []failure.FailureEvidence{ev}))
}
