package handlers

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

// fakeSanitizer is a pass-through log sanitizer.
type fakeSanitizer struct{}

func (fakeSanitizer) Sanitize(s string) string { return s }

// fakeLocator returns a fixed file path and service name, or an error, for
// NodeLocator.Locate.
type fakeLocator struct {
	filePath, serviceName string
	err                   error
}

func (f fakeLocator) Locate(_ context.Context, _ string) (string, string, error) {
	return f.filePath, f.serviceName, f.err
}

// fakePrecedents returns no precedents for any query, or an error if set. It
// exists so a trigger's precedent lookup has a non-nil port to call; the
// precedent content itself is exercised in the fixer package's own tests.
type fakePrecedents struct{ err error }

func (f fakePrecedents) Precedents(_ context.Context, _ ports.PrecedentQuery) ([]prompt.Precedent, error) {
	return nil, f.err
}

// fakeCandidateSource returns a fixed bundle source, or an error, for
// CandidateSourceReader.NodeSource. It exists so a validation trigger's
// candidate-source lookup has a non-nil port to call; the source-ladder
// behavior itself is exercised in the fixer package's own tests.
type fakeCandidateSource struct {
	src ports.CandidateSource
	err error
}

func (f fakeCandidateSource) NodeSource(_ context.Context, _, _, _ string) (ports.CandidateSource, error) {
	return f.src, f.err
}

// fakeUpstream returns fixed upstream changes, or an error, for
// UpstreamChangeReader.UpstreamChanges.
type fakeUpstream struct {
	changes []prompt.UpstreamChange
	err     error
}

func (f fakeUpstream) UpstreamChanges(_ context.Context, _ string) ([]prompt.UpstreamChange, error) {
	return f.changes, f.err
}

// fakeVersions returns a fixed current version, or an error, for
// VersionReader.CurrentVersion.
type fakeVersions struct {
	v   ports.CurrentVersion
	ok  bool
	err error
}

func (f fakeVersions) CurrentVersion(_ context.Context, _ string) (ports.CurrentVersion, bool, error) {
	return f.v, f.ok, f.err
}

// fakeLLM returns results from a queue (one per Propose call, in order).
// When the queue is exhausted, the last entry is repeated. A single-entry queue
// reproduces the original single-result behaviour. probe, when set, runs on each
// Propose call, letting a test observe repository state at the moment the model
// is invoked (e.g. that the in-flight generating row was already committed).
type fakeLLM struct {
	queue []ports.ProposeResult
	errs  []error
	calls int
	probe func()
}

func newFakeLLM(res ports.ProposeResult, err error) fakeLLM {
	return fakeLLM{queue: []ports.ProposeResult{res}, errs: []error{err}}
}

func (f *fakeLLM) Propose(_ context.Context, _ ports.ProposeRequest) (ports.ProposeResult, error) {
	if f.probe != nil {
		f.probe()
	}
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

// fakeArtifacts records writes in memory and returns deterministic URIs. The
// URI shape matters to the driver: it reads a proposed edit's content back
// through the evidence reader to build the shadow release's source overlay, so
// tests key their evidence fixtures on exactly these URIs.
type fakeArtifacts struct {
	written map[string]string
}

func (f *fakeArtifacts) Write(_ context.Context, key, body, _ string) (string, error) {
	if f.written == nil {
		f.written = map[string]string{}
	}
	f.written[key] = body
	return "s3://art/" + key, nil
}

// fakeGateway records the shadow releases the driver submits and answers with a
// scripted image tag for the failing release.
type fakeGateway struct {
	imageTag  string
	submitted []ports.ShadowSubmission
}

func (g *fakeGateway) Submit(_ context.Context, s ports.ShadowSubmission) error {
	g.submitted = append(g.submitted, s)
	return nil
}

func (g *fakeGateway) Verdict(context.Context, string) (ports.ShadowVerdict, error) {
	return ports.ShadowVerdict{}, nil
}

func (g *fakeGateway) ImageTag(context.Context, string, string) (string, error) {
	return g.imageTag, nil
}

// tarNames lists the member paths of a gzip tarball, in archive order.
func tarNames(t *testing.T, gz []byte) []string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	require.NoError(t, err)
	tr := tar.NewReader(zr)
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return names
		}
		require.NoError(t, err)
		names = append(names, h.Name)
	}
}

// fakeSource returns file content or an error. When files is non-nil it acts as
// a path→content map (misses return ports.ErrSourceNotFound); otherwise it
// returns the single content for any path. readPath records the last path
// successfully read so tests can assert the offending file was resolved. ListDir
// returns ErrSourceNotFound by default so the compile gather reads only the
// offending file (co-located siblings are best-effort).
type fakeSource struct {
	content  string
	files    map[string]string
	err      error
	readPath string
}

func (f *fakeSource) ReadFile(_ context.Context, _, _, path string) (string, error) {
	if f.files != nil {
		c, ok := f.files[path]
		if !ok {
			return "", ports.ErrSourceNotFound
		}
		f.readPath = path
		return c, nil
	}
	f.readPath = path
	return f.content, f.err
}

func (f *fakeSource) ListDir(_ context.Context, _, _, _ string) ([]string, error) {
	return nil, ports.ErrSourceNotFound
}

// fakeClock returns a fixed UTC timestamp.
type fakeClock struct{}

func (fakeClock) Now() time.Time { return time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC) }

// fakeProposalRepo satisfies repository.ProposalRepository in memory. inserted
// collects only TERMINAL Upsert calls; generating collects the in-flight
// InsertGenerating calls. genKeys models the ON CONFLICT DO NOTHING natural key
// — (release_id, attempt), since one attempt now addresses a release's whole
// failing set — so a redelivery of an in-flight attempt does not create a second
// generating row.
type fakeProposalRepo struct {
	count int
	// countByRelease, when set, answers CountAttempts per "<releaseID>#<round>"
	// key. The cap is scoped to a release and remediation round, so a fake that
	// ignored either would pass a handler that ignored it too.
	countByRelease map[string]int
	inserted       []proposal.Proposal
	generating     []proposal.Proposal
	genKeys        map[string]bool
}

func (r *fakeProposalRepo) CountAttempts(_ context.Context, releaseID string, round int) (int, error) {
	if r.countByRelease != nil {
		return r.countByRelease[fmt.Sprintf("%s#%d", releaseID, round)], nil
	}
	return r.count, nil
}

func natKey(p proposal.Proposal) string {
	return fmt.Sprintf("%s|%d", p.ReleaseID, p.Attempt)
}

func (r *fakeProposalRepo) InsertGenerating(_ context.Context, p proposal.Proposal) error {
	if r.genKeys == nil {
		r.genKeys = map[string]bool{}
	}
	k := natKey(p)
	if r.genKeys[k] {
		return nil // ON CONFLICT DO NOTHING: attempt already in flight.
	}
	r.genKeys[k] = true
	p.Status = proposal.StatusGenerating
	r.generating = append(r.generating, p)
	return nil
}

func (r *fakeProposalRepo) FailGenerating(_ context.Context, releaseID, reason string) (int, error) {
	n := 0
	for i := range r.generating {
		g := &r.generating[i]
		if g.Status != proposal.StatusGenerating || g.ReleaseID != releaseID {
			continue
		}
		g.Status = proposal.StatusFailed
		g.Rationale = reason
		n++
	}
	return n, nil
}

func (r *fakeProposalRepo) Upsert(_ context.Context, p proposal.Proposal) error {
	r.inserted = append(r.inserted, p)
	return nil
}

func (r *fakeProposalRepo) Get(_ context.Context, _ string) (proposal.View, error) {
	return proposal.View{}, repository.ErrNotFound
}

func (r *fakeProposalRepo) List(_ context.Context, _ repository.ProposalFilter) ([]proposal.View, error) {
	return nil, nil
}

func (r *fakeProposalRepo) BeginPR(_ context.Context, _, _ string, _ time.Time) (proposal.PRClaim, error) {
	return proposal.PRClaim{}, nil
}

func (r *fakeProposalRepo) RecordPR(_ context.Context, _ string, _ string, _ int, _ string, _ time.Time) (bool, error) {
	return true, nil
}

func (r *fakeProposalRepo) FailStuckOpeningPR(_ context.Context, _ string, _ time.Time) (bool, error) {
	return false, nil
}

func (r *fakeProposalRepo) ListOpenPullRequests(_ context.Context, _ int) ([]proposal.OpenPR, error) {
	return nil, nil
}

func (r *fakeProposalRepo) ListStuckOpening(_ context.Context, _ int, _ *repository.OpeningCursor) ([]proposal.OpeningPR, *repository.OpeningCursor, error) {
	return nil, nil, nil
}

func (r *fakeProposalRepo) RecordPROutcome(_ context.Context, _ string, _ proposal.PROutcome, _ time.Time) (bool, error) {
	return false, nil
}

func (r *fakeProposalRepo) ListVerifying(_ context.Context) ([]proposal.View, error) {
	return nil, nil
}

func (r *fakeProposalRepo) MarkVerified(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (r *fakeProposalRepo) MarkVerifyFailed(_ context.Context, _, _ string) (bool, error) {
	return false, nil
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

// AlreadyProcessed mirrors the two dedup axes InsertIfNotExists keys on: a row
// inserted with an outbox_entry_id is found by that id, otherwise by the
// (message_id, stream_name) pair. dedupKey stores exactly one of these axes per
// row, matching the production insert lookup.
func (r *fakeMsgProcRepo) AlreadyProcessed(
	_ context.Context, messageID, streamName string, outboxEntryID *uuid.UUID,
) (bool, error) {
	if outboxEntryID != nil {
		if _, ok := r.seen["oe:"+outboxEntryID.String()]; ok {
			return true, nil
		}
	}
	if _, ok := r.seen["mid:"+messageID+":"+streamName]; ok {
		return true, nil
	}
	return false, nil
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
		Source:           &fakeSource{},
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		ServiceRepoPaths: map[string]string{"svc": "services/svc", "other": "services/other"},
		Locator:          fakeLocator{},
		Precedents:       fakePrecedents{},
		CandidateSource:  fakeCandidateSource{err: ports.ErrNotFound},
		Upstream:         fakeUpstream{},
		Versions:         fakeVersions{},
		Releases:         &fakeGateway{imageTag: "tag-0"},
	}
}

func baseTrigger() Trigger {
	return Trigger{
		Source: "validation", ReleaseID: "r1", Repo: "o/r", CommitSHA: "abc",
		CodeBundleURI: "s3://b/bundle.json", MessageID: "1-0",
		Nodes: []TriggerNode{{NodeID: "s.n", ErrorSignature: "sig", Category: "logic", DBTLogURI: "s3://b/log",
			CandidateArtifactURI: "s3://b/sql", FilePath: "models/n.sql", Service: "svc", NodeType: "dbt-model"}},
	}
}

// TestProposeFix_CompileSource verifies the compile branch: a compile node
// carries no candidate SQL but a FilePath. The handler must read the offending
// source from version control, prompt the LLM with the dbt error + raw source,
// and record a source-resolved attempt awaiting the shadow release that will
// judge it. A non-empty proposed-fix artifact is written.
func TestProposeFix_CompileSource(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://c.log": "Compilation Error in model daily_transactions (models/daily_transactions.sql)\n  unexpected '}' in config block",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedContent: "{{ config(materialized='table', tags=['daily']) }}\nselect * from analytics.raw_transactions",
		Rationale:       "fixed malformed config block",
		Confidence:      "high",
		Model:           "m",
	}, nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-c"}
	// The broken source has a malformed config()/tags expression. Keyed by the
	// full offending path so the co-located dbt_project.yml read returns 404 and
	// does not overwrite readPath.
	src := &fakeSource{files: map[string]string{
		"models/daily_transactions.sql": "{{ config(materialized='table'), tags=['daily'])}}\nselect * from analytics.raw_transactions",
	}}

	d := Deps{
		NewUoW:      func() uow.UnitOfWork { return u },
		LLM:         &llm,
		Evidence:    ev,
		Source:      src,
		Sanitizer:   fakeSanitizer{},
		Artifacts:   art,
		Clock:       fakeClock{},
		Logger:      slog.Default(),
		MaxAttempts: 3,
		// Service "core" maps to the repo root, so the full path equals FilePath.
		ServiceRepoPaths: map[string]string{"core": ""},
		Precedents:       fakePrecedents{},
		Releases:         gw,
	}
	tr := Trigger{
		Source: "compile", ReleaseID: "r1", Repo: "o/r", CommitSHA: "sha", MessageID: "7-0",
		Nodes: []TriggerNode{{NodeID: "core", ErrorSignature: "compile-err",
			FilePath: "models/daily_transactions.sql", DBTLogURI: "s3://c.log"}},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, p.Status)
	require.Equal(t, "compile", p.Source)
	require.Equal(t, "models/daily_transactions.sql", p.FilePath)
	require.True(t, p.SourceResolved)
	require.Equal(t, "core", p.Edits[0].TargetNodeID)
	// Source must be read at join(prefix, FilePath); prefix is empty here.
	require.Equal(t, "models/daily_transactions.sql", src.readPath)

	nonEmpty := 0
	for _, body := range art.written {
		if body != "" {
			nonEmpty++
		}
	}
	require.NotZero(t, nonEmpty, "expected a non-empty proposed-fix artifact")
	// A service rooted at the repository root still owns the edit, so the shadow
	// release lays the file down at the path the project already uses.
	require.Len(t, gw.submitted, 1)
	require.Equal(t, "shadow-r1-core-a1", gw.submitted[0].ReleaseID)
	require.Equal(t, []string{"models/daily_transactions.sql"},
		tarNames(t, []byte(art.written["core/shadow-r1-core-a1/source-overlay.tar.gz"])))
	require.Empty(t, u.ob.entries, "the driver announces nothing; the reconciler does once the shadow validates")
	require.True(t, u.committed)
}

// TestProposeFix_SeedSourceViaThreadedPayload verifies the primary seed_build
// path: when FilePath and Service are carried on the node (threaded from the
// candidate topology), the handler must produce a source-resolved attempt
// without a NodeLocator call. This is the common case for newly-added seeds
// that do not yet exist in the promoted topology the NodeLocator serves.
func TestProposeFix_SeedSourceViaThreadedPayload(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/log": "Database Error in seed customers: extra column",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedContent: "id,name\n1,alice",
		Rationale:       "removed extra column",
		Confidence:      "high",
		Model:           "m",
	}, nil)
	art := &fakeArtifacts{}
	src := &fakeSource{content: "id,name\n1,alice,extra"}
	gw := &fakeGateway{imageTag: "tag-s"}

	d := Deps{
		NewUoW:   func() uow.UnitOfWork { return u },
		LLM:      &llm,
		Evidence: ev,
		// Locator is deliberately left unset: FilePath and Service are threaded on
		// the node, so the NodeLocator fallback must never be called — a call
		// here would panic on the nil port.
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
		Precedents:       fakePrecedents{},
		Releases:         gw,
	}
	tr := Trigger{
		Source: "seed_build", ReleaseID: "r1", Repo: "o/r", CommitSHA: "sha", MessageID: "8-0",
		Nodes: []TriggerNode{{NodeID: "svc.customers", ErrorSignature: "seed-err",
			// FilePath and Service are threaded from the candidate topology by
			// release-controller, bypassing the need for a NodeLocator call.
			FilePath: "seeds/customers.csv", Service: "svc", DBTLogURI: "s3://b/log"}},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, p.Status)
	require.Equal(t, "seed_build", p.Source)
	require.True(t, p.SourceResolved)
	require.Equal(t, "services/svc/seeds/customers.csv", p.FilePath)
	require.Equal(t, "services/svc/seeds/customers.csv", src.readPath)
	require.Equal(t, []string{"seeds/customers.csv"},
		tarNames(t, []byte(art.written["svc/shadow-r1-svc-a1/source-overlay.tar.gz"])),
		"an overlay member is relative to the service's own project, not the repository")
	require.Empty(t, u.ob.entries)
}

// TestProposeFix_SeedSourceFallsBackToLocator verifies the NodeLocator
// fallback path for seed_build: when FilePath is absent on the node (e.g.
// an older rejection payload that predates the candidate-topology threading),
// the handler falls back to the orchestrator graph's NodeLocator to resolve
// the source location.
func TestProposeFix_SeedSourceFallsBackToLocator(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/log": "Database Error in seed customers: extra column",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedContent: "id,name\n1,alice",
		Rationale:       "removed extra column",
		Confidence:      "high",
		Model:           "m",
	}, nil)
	art := &fakeArtifacts{}
	src := &fakeSource{content: "id,name\n1,alice,extra"}

	d := Deps{
		NewUoW:           func() uow.UnitOfWork { return u },
		LLM:              &llm,
		Evidence:         ev,
		Locator:          fakeLocator{filePath: "seeds/customers.csv", serviceName: "svc"},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
		Precedents:       fakePrecedents{},
		Releases:         &fakeGateway{imageTag: "tag-s"},
	}
	// No FilePath or Service on the node: must fall back to the NodeLocator.
	tr := Trigger{
		Source: "seed_build", ReleaseID: "r1", Repo: "o/r", CommitSHA: "sha", MessageID: "8-1",
		Nodes: []TriggerNode{{NodeID: "svc.customers", ErrorSignature: "seed-err", DBTLogURI: "s3://b/log"}},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, p.Status)
	require.True(t, p.SourceResolved)
	require.Equal(t, "services/svc/seeds/customers.csv", p.FilePath)
}

// TestProposeFix_SeedFilePathSetServiceEmptyResolvesViaLocator covers the
// partial-threading case: a topology node carries a file_path but no service.
// The handler must fall back to the NodeLocator for the SERVICE (not treat
// NodeID as the service) while keeping the threaded file_path — otherwise the
// attempt is wrongly skipped.
func TestProposeFix_SeedFilePathSetServiceEmptyResolvesViaLocator(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/log": "Database Error in seed customers: extra column",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedContent: "id,name\n1,alice",
		Rationale:       "removed extra column",
		Confidence:      "high",
		Model:           "m",
	}, nil)
	art := &fakeArtifacts{}
	src := &fakeSource{content: "id,name\n1,alice,extra"}

	d := Deps{
		NewUoW:   func() uow.UnitOfWork { return u },
		LLM:      &llm,
		Evidence: ev,
		// The NodeLocator supplies the missing service; its filePath is
		// deliberately wrong to prove the threaded file_path is preserved, not
		// overwritten.
		Locator:          fakeLocator{filePath: "seeds/WRONG.csv", serviceName: "svc"},
		Source:           src,
		Sanitizer:        fakeSanitizer{},
		Artifacts:        art,
		Clock:            fakeClock{},
		Logger:           slog.Default(),
		MaxAttempts:      3,
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
		Precedents:       fakePrecedents{},
		Releases:         &fakeGateway{imageTag: "tag-s"},
	}
	tr := Trigger{
		Source: "seed_build", ReleaseID: "r1", Repo: "o/r", CommitSHA: "sha", MessageID: "9-0",
		Nodes: []TriggerNode{{
			NodeID:         "svc.customers", // a dbt node id, NOT a service
			ErrorSignature: "seed-err",
			FilePath:       "seeds/customers.csv", // threaded; service missing
			DBTLogURI:      "s3://b/log",
		}},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusVerifying, u.pr.inserted[0].Status)
	// Threaded file_path preserved + service resolved from the NodeLocator → svc.
	require.Equal(t, "services/svc/seeds/customers.csv", src.readPath)
}

// sourceFixLLM scripts the two calls a dbt validation fix makes: the diagnosis
// on the candidate SQL, then the same diagnosis applied to the real source.
func sourceFixLLM(proposed, rationale, confidence string) fakeLLM {
	res := ports.ProposeResult{ProposedSQL: proposed, Rationale: rationale, Confidence: confidence, Model: "m"}
	return fakeLLM{queue: []ports.ProposeResult{res, res}}
}

func TestProposeFix_HappyPath(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.n/attempt-1.source.sql": "select customer_id from t",
	}}
	llm := sourceFixLLM("select customer_id from t", "typo", "high")
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, art)
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select custmer_id from t", Runtime: ports.RuntimeDbt}}

	require.NoError(t, ProposeFix(context.Background(), d, baseTrigger()))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, p.Status)
	require.Equal(t, "s.n", p.NodeID)
	require.Equal(t, []string{"s.n"}, p.ResolvedNodeIDs)
	require.Equal(t, "sig", p.ErrorSignature)
	require.Equal(t, proposal.ConfidenceHigh, p.Confidence)
	require.Equal(t, "services/svc/models/n.sql", p.FilePath)
	require.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.n"].Status)
	require.Equal(t, []proposal.Verification{{
		Service: "svc", Kind: ports.ShadowKindDbt, ShadowReleaseID: "shadow-r1-svc-a1",
	}}, p.Verifications)
	require.Equal(t, "shadow-r1-svc-a1", p.ShadowReleaseID, "the representative shadow is the first verification's")

	require.Len(t, gw.submitted, 1)
	require.Equal(t, "tag-1", gw.submitted[0].ImageTag)
	require.Equal(t, "s3://art/svc/shadow-r1-svc-a1/source-overlay.tar.gz", gw.submitted[0].SourceOverlayURI)
	require.Empty(t, u.ob.entries, "an unverified fix must not be announced")
	require.True(t, u.committed)
}

func TestProposeFix_AttemptCapEscalates(t *testing.T) {
	u := newFakeUoW()
	u.pr.count = 3
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}
	llm := newFakeLLM(ports.ProposeResult{}, nil)

	require.NoError(t, ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), baseTrigger()))

	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusEscalated, u.pr.inserted[0].Status)
	require.Empty(t, u.ob.entries, "escalated must not emit an outbox entry")
}

// TestProposeFix_AttemptCapEscalatesEveryNode: the cap is a budget for the whole
// release, so exhausting it escalates every node the trigger carries, in one
// row, without a model call or an in-flight row.
func TestProposeFix_AttemptCapEscalatesEveryNode(t *testing.T) {
	u := newFakeUoW()
	u.pr.count = 3
	llm := newFakeLLM(ports.ProposeResult{}, nil)
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, fakeEvidence{}, &llm, &fakeArtifacts{})
	d.Releases = gw
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.b", ErrorSignature: "sig-b", Service: "svc", NodeType: "dbt-model"},
		{NodeID: "s.a", ErrorSignature: "sig-a", Service: "svc", NodeType: "dbt-model"},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	assert.Equal(t, proposal.StatusEscalated, p.Status)
	assert.Equal(t, []string{"s.a", "s.b"}, p.ResolvedNodeIDs)
	for _, id := range []string{"s.a", "s.b"} {
		assert.Equal(t, proposal.StatusEscalated, p.NodeOutcomes[id].Status)
	}
	assert.Empty(t, u.pr.generating, "an escalated attempt never marks generating")
	assert.Zero(t, llm.calls)
	assert.Empty(t, gw.submitted)
}

// TestProposeFix_AttemptCapIsScopedToTheRelease: three exhausted attempts on
// one release do not bind a later release. The same failure under a new release
// is new code, and gets its own budget: the model is consulted and the attempt
// numbering starts again at 1. The release that exhausted its budget stays
// escalated.
func TestProposeFix_AttemptCapIsScopedToTheRelease(t *testing.T) {
	u := newFakeUoW()
	u.pr.countByRelease = map[string]int{"r1#1": 3}
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r2/s.n/attempt-1.source.sql": "select customer_id from t",
	}}
	llm := sourceFixLLM("select customer_id from t", "typo", "high")
	d := deps(u, ev, &llm, &fakeArtifacts{})
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select custmer_id from t", Runtime: ports.RuntimeDbt}}

	later := baseTrigger()
	later.ReleaseID = "r2"
	require.NoError(t, ProposeFix(context.Background(), d, later))

	require.Equal(t, 2, llm.calls, "a new release must reach the model")
	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusVerifying, u.pr.inserted[0].Status)
	require.Equal(t, 1, u.pr.inserted[0].Attempt)

	// The exhausted release is still escalated, without another model call.
	u2 := newFakeUoW()
	u2.pr.countByRelease = map[string]int{"r1#1": 3}
	llm2 := newFakeLLM(ports.ProposeResult{}, nil)
	require.NoError(t, ProposeFix(context.Background(), deps(u2, ev, &llm2, &fakeArtifacts{}), baseTrigger()))
	require.Zero(t, llm2.calls)
	require.Len(t, u2.pr.inserted, 1)
	require.Equal(t, proposal.StatusEscalated, u2.pr.inserted[0].Status)
}

// TestProposeFix_RoundTwoStartsAFreshBudget: the same release and failing set
// under remediation round 2 is a new budget, and the row records the round.
func TestProposeFix_RoundTwoStartsAFreshBudget(t *testing.T) {
	u := newFakeUoW()
	u.pr.countByRelease = map[string]int{"r1#1": 3}
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.n/attempt-4.source.sql": "select customer_id from t",
	}}
	llm := sourceFixLLM("select customer_id from t", "typo", "high")
	d := deps(u, ev, &llm, &fakeArtifacts{})
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select custmer_id from t", Runtime: ports.RuntimeDbt}}

	tr := baseTrigger()
	tr.RemediationRound = 2
	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Equal(t, 2, llm.calls, "round 2 must reach the model")
	row := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, row.Status)
	require.Equal(t, 2, row.RemediationRound)
	require.Equal(t, 2, u.pr.generating[0].RemediationRound, "the generating row must carry the round")
}

// TestProposeFix_AttemptNumberKeepsIncrementingAcrossRounds guards the
// natural-key collision a naive round-scoped attempt count would cause: the
// proposal table is unique on (release_id, attempt), not scoped by
// remediation_round, so a later round's first attempt must continue the
// release's existing attempt sequence rather than restart it at 1 — which
// would collide with, and silently overwrite via the terminal upsert's ON
// CONFLICT, the row an earlier round already wrote. Round 1 already used 3
// terminal attempts (e.g. it escalated); round 2's own budget is fresh (0
// attempts), but the attempt NUMBER assigned must be 4.
func TestProposeFix_AttemptNumberKeepsIncrementingAcrossRounds(t *testing.T) {
	u := newFakeUoW()
	u.pr.countByRelease = map[string]int{"r1#1": 3}
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "select custmer_id from t", "s3://b/log": "column does not exist"}}
	llm := newFakeLLM(ports.ProposeResult{ProposedSQL: "select customer_id from t", Rationale: "typo", Confidence: "high", Model: "m"}, nil)

	tr := baseTrigger()
	tr.RemediationRound = 2
	require.NoError(t, ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), tr))

	require.Equal(t, 4, u.pr.generating[0].Attempt,
		"the generating row must continue the release's attempt sequence")
	require.Equal(t, 4, u.pr.inserted[0].Attempt,
		"the terminal row must continue the release's attempt sequence")
}

func TestProposeFix_EmptyCandidateSQLSkips(t *testing.T) {
	u := newFakeUoW()
	tr := baseTrigger()
	tr.Nodes[0].CandidateArtifactURI = ""
	llm := newFakeLLM(ports.ProposeResult{}, nil)
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, fakeEvidence{}, &llm, &fakeArtifacts{})
	d.Releases = gw

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusSkipped, u.pr.inserted[0].Status)
	require.Equal(t, proposal.StatusSkipped, u.pr.inserted[0].NodeOutcomes["s.n"].Status)
	require.Empty(t, u.ob.entries)
	require.Empty(t, gw.submitted, "a skip has nothing to verify")
}

// TestProposeFix_EmptyCandidateSQLSkipsDespiteLogError proves the
// empty-candidate validation skip is decided before any dbt-log read: even when
// the evidence store returns a transient error for every fetch (an unreadable or
// misconfigured log URI), the trigger is still recorded skipped and ACKed rather
// than erroring and being redelivered.
func TestProposeFix_EmptyCandidateSQLSkipsDespiteLogError(t *testing.T) {
	u := newFakeUoW()
	tr := baseTrigger()
	tr.Nodes[0].CandidateArtifactURI = ""
	llm := newFakeLLM(ports.ProposeResult{}, nil)
	ev := fakeEvidence{err: fmt.Errorf("s3 503: log temporarily unreadable")}

	require.NoError(t, ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), tr),
		"an empty-candidate skip must not surface a log-read error")
	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusSkipped, u.pr.inserted[0].Status)
	require.Empty(t, u.ob.entries)
}

func TestProposeFix_LLMEmptyFails(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}
	llm := newFakeLLM(ports.ProposeResult{ProposedSQL: ""}, nil)

	require.NoError(t, ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), baseTrigger()))

	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusFailed, u.pr.inserted[0].Status)
	require.Equal(t, proposal.StatusFailed, u.pr.inserted[0].NodeOutcomes["s.n"].Status)
	require.Empty(t, u.ob.entries)
}

// TestProposeFix_DuplicateTriggerIsDeduped calls ProposeFix twice with
// identical trigger data (same MessageID / OutboxEntryID). The second call
// must be recognised as a duplicate and return nil without inserting a second
// proposal row or submitting a second shadow release.
func TestProposeFix_DuplicateTriggerIsDeduped(t *testing.T) {
	// Both calls share the same UoW factory state (fakeMsgProcRepo is reused
	// across calls, which is what the fake is designed for).
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.n/attempt-1.source.sql": "select customer_id from t",
	}}
	llm := sourceFixLLM("select customer_id from t", "typo", "high")
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, &fakeArtifacts{})
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select custmer_id from t", Runtime: ports.RuntimeDbt}}

	oeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	tr := baseTrigger()
	tr.MessageID = "42-0"
	tr.OutboxEntryID = &oeID

	require.NoError(t, ProposeFix(context.Background(), d, tr))
	require.Len(t, u.pr.inserted, 1)
	require.Len(t, gw.submitted, 1)

	// Second call with the same trigger (simulates redelivery).
	require.NoError(t, ProposeFix(context.Background(), d, tr), "a duplicate must return nil")

	require.Len(t, u.pr.inserted, 1, "the duplicate must not write a second row")
	require.Len(t, gw.submitted, 1, "the duplicate must not submit a second shadow release")
}

// TestProposeFix_MarksGeneratingBeforeLLM verifies the in-flight indicator
// contract: an in-flight 'generating' row is committed BEFORE the model is
// called, and is finalized afterwards. The probe runs inside the LLM call and
// asserts the generating row already exists at that moment, naming the whole
// failing set it is working on.
func TestProposeFix_MarksGeneratingBeforeLLM(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t", Rationale: "typo", Confidence: "high", Model: "m",
	}, nil)

	var genAtLLMCall int
	llm.probe = func() { genAtLLMCall = len(u.pr.generating) }

	require.NoError(t, ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), baseTrigger()))

	require.Equal(t, 1, genAtLLMCall, "the generating row must be committed before the model is called")
	require.Len(t, u.pr.generating, 1)
	require.Equal(t, proposal.StatusGenerating, u.pr.generating[0].Status)
	require.Equal(t, 1, u.pr.generating[0].Attempt)
	require.Equal(t, []string{"s.n"}, u.pr.generating[0].ResolvedNodeIDs)
	require.Equal(t, proposal.StatusGenerating, u.pr.generating[0].NodeOutcomes["s.n"].Status)
	require.Len(t, u.pr.inserted, 1, "the terminal write finalizes the same attempt")
	require.Equal(t, 1, u.pr.inserted[0].Attempt)
}

// TestProposeFix_MarksOneGeneratingRowForTheWholeFailingSet pins the batched
// in-flight row: several failing nodes are one attempt, so the release page has
// one row to show, not one per node.
func TestProposeFix_MarksOneGeneratingRowForTheWholeFailingSet(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(ports.ProposeResult{}, nil)
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.b", ErrorSignature: "sig-b", Service: "svc", NodeType: "dbt-model"},
		{NodeID: "s.a", ErrorSignature: "sig-a", Service: "svc", NodeType: "dbt-model"},
	}

	require.NoError(t, ProposeFix(context.Background(), deps(u, fakeEvidence{}, &llm, &fakeArtifacts{}), tr))

	require.Len(t, u.pr.generating, 1)
	require.Equal(t, []string{"s.a", "s.b"}, u.pr.generating[0].ResolvedNodeIDs)
	require.Equal(t, "s.a", u.pr.generating[0].NodeID, "the representative is the smallest resolved id")
	require.Equal(t, "sig-a", u.pr.generating[0].ErrorSignature, "the signature is the representative node's")
}

// TestProposeFix_AlreadyProcessedPreCheckNoPhantom proves the read-only dedup
// pre-check prevents a phantom generating row. A completed trigger is re-emitted
// with a fresh Redis message id but the same upstream outbox_entry_id; the second
// invocation must ACK (return nil) before writing anything — no extra generating
// row, no extra terminal row, and no second model call.
func TestProposeFix_AlreadyProcessedPreCheckNoPhantom(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	llm := newFakeLLM(ports.ProposeResult{
		ProposedSQL: "select customer_id from t", Rationale: "typo", Confidence: "high", Model: "m",
	}, nil)
	d := deps(u, ev, &llm, &fakeArtifacts{})

	oeID := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	tr := baseTrigger()
	tr.MessageID = "100-0"
	tr.OutboxEntryID = &oeID

	require.NoError(t, ProposeFix(context.Background(), d, tr))
	require.Len(t, u.pr.generating, 1)
	require.Len(t, u.pr.inserted, 1)
	callsAfterFirst := llm.calls

	// Re-emission: fresh message id, same outbox_entry_id.
	tr.MessageID = "200-0"
	require.NoError(t, ProposeFix(context.Background(), d, tr), "a re-emitted trigger must return nil")

	require.Len(t, u.pr.generating, 1, "the re-emission must not add a phantom generating row")
	require.Len(t, u.pr.inserted, 1, "the re-emission must not add a terminal row")
	require.Equal(t, callsAfterFirst, llm.calls, "the re-emission must not call the model again")
}

// TestProposeFix_EscalateWritesNoGenerating verifies the attempt-cap path never
// shows the "Generating fix…" chip: it escalates before markGenerating, so no
// generating row is written and the model is never called.
func TestProposeFix_EscalateWritesNoGenerating(t *testing.T) {
	u := newFakeUoW()
	u.pr.count = 3
	ev := fakeEvidence{vals: map[string]string{"s3://b/sql": "x", "s3://b/log": "y"}}
	llm := newFakeLLM(ports.ProposeResult{}, nil)

	require.NoError(t, ProposeFix(context.Background(), deps(u, ev, &llm, &fakeArtifacts{}), baseTrigger()))

	require.Empty(t, u.pr.generating, "an escalated attempt must not write a generating row")
	require.Zero(t, llm.calls)
	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusEscalated, u.pr.inserted[0].Status)
}

// TestProposeFix_InternalSkipFinalizesGenerating documents the accepted
// generating→blank flicker: a Fixer that skips internally (here, a validation
// node with no candidate SQL) still marks the attempt generating in the driver,
// then finalizes it to skipped. Exactly one generating row and one skipped
// terminal row result.
func TestProposeFix_InternalSkipFinalizesGenerating(t *testing.T) {
	u := newFakeUoW()
	tr := baseTrigger()
	tr.Nodes[0].CandidateArtifactURI = ""
	llm := newFakeLLM(ports.ProposeResult{}, nil)

	require.NoError(t, ProposeFix(context.Background(), deps(u, fakeEvidence{}, &llm, &fakeArtifacts{}), tr))

	require.Len(t, u.pr.generating, 1)
	require.Len(t, u.pr.inserted, 1)
	require.Equal(t, proposal.StatusSkipped, u.pr.inserted[0].Status)
	require.Empty(t, u.ob.entries)
}

// TestProposeFix_NoNodesIsAcknowledged: a trigger with an empty failing set has
// nothing to fix and must be ACKed rather than writing an attempt no node
// belongs to.
func TestProposeFix_NoNodesIsAcknowledged(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(ports.ProposeResult{}, nil)
	tr := baseTrigger()
	tr.Nodes = nil

	require.NoError(t, ProposeFix(context.Background(), deps(u, fakeEvidence{}, &llm, &fakeArtifacts{}), tr))

	require.Empty(t, u.pr.inserted)
	require.Empty(t, u.pr.generating)
	require.Zero(t, llm.calls)
}

// TestProposeFix_TwoIndependentNodes_OneVerifyingProposalWithTwoEdits is the
// shape of a batched attempt: two unrelated failures in one service become one
// proposal with two edits and one shadow release carrying both.
func TestProposeFix_TwoIndependentNodes_OneVerifyingProposalWithTwoEdits(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql-a": "select a", "s3://b/sql-b": "select b", "s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.a/attempt-1.source.sql": "select a2",
		"s3://art/proposed-fix/r1/s.b/attempt-1.source.sql": "select b2",
	}}
	llm := fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "select a2", Rationale: "a", Confidence: "high", Model: "m"}, // step 1 for a
		{ProposedSQL: "select a2", Rationale: "a", Confidence: "high", Model: "m"}, // step 2 for a
		{ProposedSQL: "select b2", Rationale: "b", Confidence: "medium", Model: "m"},
		{ProposedSQL: "select b2", Rationale: "b", Confidence: "medium", Model: "m"},
	}}
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, art)
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select x", Runtime: ports.RuntimeDbt}}
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.b", ErrorSignature: "sig-b", Category: "logic", DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-b", FilePath: "models/b.sql", Service: "svc", NodeType: "dbt-model"},
		{NodeID: "s.a", ErrorSignature: "sig-a", Category: "logic", DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-a", FilePath: "models/a.sql", Service: "svc", NodeType: "dbt-model"},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1, "one proposal per release, not per node")
	p := u.pr.inserted[0]
	assert.Equal(t, proposal.StatusVerifying, p.Status)
	assert.Equal(t, 1, p.Attempt)
	assert.Equal(t, []string{"s.a", "s.b"}, p.ResolvedNodeIDs)
	assert.Equal(t, "s.a", p.NodeID, "representative node is the smallest resolved id")
	require.Len(t, p.Edits, 2)
	assert.Equal(t, "services/svc/models/a.sql", p.Edits[0].Path)
	assert.Equal(t, "s.a", p.Edits[0].TargetNodeID)
	assert.Equal(t, "services/svc/models/b.sql", p.Edits[1].Path)
	assert.Equal(t, proposal.ConfidenceMedium, p.Confidence, "the proposal's confidence is the lowest cluster's")
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.a"].Status)
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.b"].Status)
	require.Len(t, p.Verifications, 1, "both edits are in one service: one shadow")
	assert.Equal(t, proposal.Verification{Service: "svc", Kind: ports.ShadowKindDbt, ShadowReleaseID: "shadow-r1-svc-a1"}, p.Verifications[0])
	assert.Equal(t, tr.RawPayload, p.TriggerPayload)

	require.Len(t, gw.submitted, 1)
	sub := gw.submitted[0]
	assert.Equal(t, "shadow-r1-svc-a1", sub.ReleaseID)
	assert.Equal(t, "tag-1", sub.ImageTag)
	assert.Equal(t, ports.ShadowKindDbt, sub.Kind)
	assert.Equal(t, "s3://art/svc/shadow-r1-svc-a1/source-overlay.tar.gz", sub.SourceOverlayURI)
	overlayBytes := art.written["svc/shadow-r1-svc-a1/source-overlay.tar.gz"]
	names := tarNames(t, []byte(overlayBytes))
	assert.Equal(t, []string{"models/a.sql", "models/b.sql"}, names, "overlay paths are project-relative")
	assert.Empty(t, u.ob.entries, "the driver announces nothing; the reconciler does once the shadow validates")
	assert.Equal(t, 4, llm.calls, "one two-step fix per independent cluster")
}

// upstreamTrigger is the shape shared-upstream grouping recognizes: two nodes
// failing the same way, both descending from one ancestor this release changed.
func upstreamTrigger() Trigger {
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.v", ErrorSignature: "sig", Category: "logic", ErrorExcerpt: "column u.amount does not exist",
			DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-v", FilePath: "models/v.sql",
			Service: "svc", NodeType: "dbt-model", ChangedAncestorIDs: []string{"s.u"}},
		{NodeID: "s.w", ErrorSignature: "sig", Category: "logic", ErrorExcerpt: "column u.amount does not exist",
			DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-w", FilePath: "models/w.sql",
			Service: "svc", NodeType: "dbt-model", ChangedAncestorIDs: []string{"s.u"}},
	}
	return tr
}

func TestProposeFix_SharedUpstream_OneEditToTheAncestor(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{"s3://b/log": "column u.amount does not exist",
		"s3://art/proposed-fix/r1/s.u/attempt-1.source.sql": "select id, amount from s.base"}}
	llm := newFakeLLM(ports.ProposeResult{ProposedSQL: "select id, amount from s.base", Rationale: "restored amount", Confidence: "high", Model: "m"}, nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, art)
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select id from s.base", Runtime: ports.RuntimeDbt}}
	d.Versions = fakeVersions{v: ports.CurrentVersion{RawCode: "select id, amount from s.base"}, ok: true}
	d.Locator = fakeLocator{filePath: "models/u.sql", serviceName: "svc"}

	require.NoError(t, ProposeFix(context.Background(), d, upstreamTrigger()))

	p := u.pr.inserted[0]
	assert.Equal(t, 1, llm.calls, "one call for the whole cluster")
	require.Len(t, p.Edits, 1)
	assert.Equal(t, "s.u", p.Edits[0].TargetNodeID)
	assert.Equal(t, "services/svc/models/u.sql", p.Edits[0].Path)
	assert.Equal(t, []string{"s.v", "s.w"}, p.ResolvedNodeIDs)
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.v"].Status)
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.w"].Status)
	require.Len(t, gw.submitted, 1)
	assert.Equal(t, []string{"models/u.sql"},
		tarNames(t, []byte(art.written["svc/shadow-r1-svc-a1/source-overlay.tar.gz"])),
		"the ancestor's corrected source is what the shadow release runs")
}

// TestProposeFix_UpstreamTargetUnlocatable_FallsBackToIndependent: the changed
// ancestor cannot be placed in the promoted graph (a brand-new node), so the
// upstream fix skips and each member is repaired in its own source instead.
func TestProposeFix_UpstreamTargetUnlocatable_FallsBackToIndependent(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/log": "column u.amount does not exist", "s3://b/sql-v": "select id from s.v", "s3://b/sql-w": "select id from s.w",
		"s3://art/proposed-fix/r1/s.v/attempt-1.source.sql": "select id, amount from s.base",
		"s3://art/proposed-fix/r1/s.w/attempt-1.source.sql": "select id, amount from s.base",
	}}
	llm := newFakeLLM(ports.ProposeResult{ProposedSQL: "select id, amount from s.base", Rationale: "restored amount", Confidence: "high", Model: "m"}, nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, art)
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select id from s.base", Runtime: ports.RuntimeDbt}}
	d.Versions = fakeVersions{v: ports.CurrentVersion{RawCode: "select id, amount from s.base"}, ok: true}
	d.Locator = fakeLocator{err: fmt.Errorf("node s.u is not in the promoted graph")}

	require.NoError(t, ProposeFix(context.Background(), d, upstreamTrigger()))

	p := u.pr.inserted[0]
	assert.Equal(t, 4, llm.calls, "each member falls back to its own two-step fix")
	require.Len(t, p.Edits, 2)
	assert.Equal(t, "s.v", p.Edits[0].TargetNodeID)
	assert.Equal(t, "services/svc/models/v.sql", p.Edits[0].Path)
	assert.Equal(t, "s.w", p.Edits[1].TargetNodeID)
	assert.Equal(t, "services/svc/models/w.sql", p.Edits[1].Path)
	assert.Equal(t, proposal.StatusVerifying, p.Status)
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.v"].Status)
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.w"].Status)
}

// TestProposeFix_MixedOutcomes_SkippedMemberDoesNotBlockVerification: one node
// this agent cannot fix must not hold back the fix for the node it can.
func TestProposeFix_MixedOutcomes_SkippedMemberDoesNotBlockVerification(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql-b": "select b", "s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.b/attempt-1.source.sql": "select b2",
	}}
	llm := fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "select b2", Rationale: "b", Confidence: "high", Model: "m"},
		{ProposedSQL: "select b2", Rationale: "b", Confidence: "high", Model: "m"},
	}}
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, art)
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select x", Runtime: ports.RuntimeDbt}}
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		// s.a carries no candidate artifact, so its fix skips before any model call.
		{NodeID: "s.a", ErrorSignature: "sig-a", Category: "logic", DBTLogURI: "s3://b/log", FilePath: "models/a.sql", Service: "svc", NodeType: "dbt-model"},
		{NodeID: "s.b", ErrorSignature: "sig-b", Category: "logic", DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-b", FilePath: "models/b.sql", Service: "svc", NodeType: "dbt-model"},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	assert.Equal(t, proposal.StatusVerifying, p.Status)
	assert.Equal(t, proposal.StatusSkipped, p.NodeOutcomes["s.a"].Status)
	assert.Equal(t, proposal.StatusVerifying, p.NodeOutcomes["s.b"].Status)
	assert.Equal(t, []string{"s.a", "s.b"}, p.ResolvedNodeIDs)
	require.Len(t, p.Edits, 1)
	assert.Equal(t, "services/svc/models/b.sql", p.Edits[0].Path)
	require.Len(t, p.Verifications, 1)
	require.Len(t, gw.submitted, 1)
	assert.Equal(t, []string{"models/b.sql"},
		tarNames(t, []byte(art.written["svc/shadow-r1-svc-a1/source-overlay.tar.gz"])),
		"the skipped node contributes nothing to the shadow release")
}

// TestProposeFix_AllSkipped_RecordsSkippedWithoutShadow: nothing was fixed, so
// no release slot is spent and the attempt records why for each node.
func TestProposeFix_AllSkipped_RecordsSkippedWithoutShadow(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(ports.ProposeResult{}, nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, fakeEvidence{}, &llm, art)
	d.Releases = gw
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.a", ErrorSignature: "sig-a", Category: "logic", DBTLogURI: "s3://b/log", FilePath: "models/a.sql", Service: "svc", NodeType: "dbt-model"},
		{NodeID: "s.b", ErrorSignature: "sig-b", Category: "logic", DBTLogURI: "s3://b/log", FilePath: "models/b.sql", Service: "svc", NodeType: "dbt-model"},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	assert.Equal(t, proposal.StatusSkipped, p.Status)
	assert.Equal(t, proposal.StatusSkipped, p.NodeOutcomes["s.a"].Status)
	assert.Equal(t, proposal.StatusSkipped, p.NodeOutcomes["s.b"].Status)
	assert.Empty(t, p.Verifications)
	assert.Empty(t, gw.submitted)
	assert.Empty(t, art.written, "a skipped attempt writes no artifact")
	assert.Zero(t, llm.calls)
}

// TestProposeFix_TwoServices_OneShadowEach: a release's failing set can span
// services, and a shadow release verifies exactly one service, so each edited
// service gets its own.
func TestProposeFix_TwoServices_OneShadowEach(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql-a": "select a", "s3://b/sql-b": "select b", "s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.a/attempt-1.source.sql": "select a2",
		"s3://art/proposed-fix/r1/s.b/attempt-1.source.sql": "select b2",
	}}
	llm := fakeLLM{queue: []ports.ProposeResult{
		{ProposedSQL: "select a2", Rationale: "a", Confidence: "high", Model: "m"},
		{ProposedSQL: "select a2", Rationale: "a", Confidence: "high", Model: "m"},
		{ProposedSQL: "select b2", Rationale: "b", Confidence: "high", Model: "m"},
		{ProposedSQL: "select b2", Rationale: "b", Confidence: "high", Model: "m"},
	}}
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "tag-1"}
	d := deps(u, ev, &llm, art)
	d.Releases = gw
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select x", Runtime: ports.RuntimeDbt}}
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.a", ErrorSignature: "sig-a", Category: "logic", DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-a", FilePath: "models/a.sql", Service: "svc", NodeType: "dbt-model"},
		{NodeID: "s.b", ErrorSignature: "sig-b", Category: "logic", DBTLogURI: "s3://b/log", CandidateArtifactURI: "s3://b/sql-b", FilePath: "models/b.sql", Service: "other", NodeType: "dbt-model"},
	}

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	p := u.pr.inserted[0]
	assert.Equal(t, proposal.StatusVerifying, p.Status)
	require.Len(t, p.Verifications, 2)
	assert.Equal(t, proposal.Verification{Service: "other", Kind: ports.ShadowKindDbt, ShadowReleaseID: "shadow-r1-other-a1"}, p.Verifications[0])
	assert.Equal(t, proposal.Verification{Service: "svc", Kind: ports.ShadowKindDbt, ShadowReleaseID: "shadow-r1-svc-a1"}, p.Verifications[1])
	assert.Equal(t, "shadow-r1-other-a1", p.ShadowReleaseID)

	require.Len(t, gw.submitted, 2)
	assert.Equal(t, "other", gw.submitted[0].Service)
	assert.Equal(t, "svc", gw.submitted[1].Service)
	assert.Equal(t, []string{"models/b.sql"}, tarNames(t, []byte(art.written["other/shadow-r1-other-a1/source-overlay.tar.gz"])))
	assert.Equal(t, []string{"models/a.sql"}, tarNames(t, []byte(art.written["svc/shadow-r1-svc-a1/source-overlay.tar.gz"])))
}

// TestProposeFix_Step2_SourceResolved verifies that when the node's location and
// the source reader both answer, the Step-2 LLM call produces source artifacts:
// the attempt's edit points at them, and the candidate artifacts are written for
// audit alongside.
func TestProposeFix_Step2_SourceResolved(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.n/attempt-1.source.sql": "{{ config(materialized='table') }}\nselect customer_id from {{ ref('t') }}",
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
	d := deps(u, ev, &llm, art)
	d.Source = src
	d.Locator = fakeLocator{filePath: "models/table_e.sql", serviceName: "svc"}
	d.Releases = &fakeGateway{imageTag: "tag-1"}
	tr := baseTrigger()
	tr.Nodes[0].FilePath = "" // no threaded path: the locator answers instead

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, p.Status)
	require.True(t, p.SourceResolved)
	// The source must be read at the full path: prefix + filePath.
	require.Equal(t, "services/svc/models/table_e.sql", src.readPath)
	require.True(t, strings.HasSuffix(p.ProposedSQLURI, "attempt-1.source.sql"),
		"the attempt's content URI must point at the source artifact")

	for _, key := range []string{
		"proposed-fix/r1/s.n/attempt-1.sql",
		"proposed-fix/r1/s.n/attempt-1.diff",
		"proposed-fix/r1/s.n/attempt-1.source.sql",
		"proposed-fix/r1/s.n/attempt-1.source.diff",
	} {
		require.Contains(t, art.written, key)
	}
}

// TestProposeFix_CandidateOnlyFixCannotBeVerified covers every way Step 2
// degrades to a candidate-only answer: the source read fails, the location is
// unknown, the service is unmapped, or the model returns nothing better. None
// of them produces a file edit, so there is nothing a shadow release could run
// and nothing a pull request could carry — the attempt fails, and no release
// slot is spent on it.
func TestProposeFix_CandidateOnlyFixCannotBeVerified(t *testing.T) {
	const originalSQL = "{{ config(materialized='table') }}\nselect custmer_id from {{ ref('t') }}"
	step1 := ports.ProposeResult{ProposedSQL: "select customer_id from t", Rationale: "typo fix", Confidence: "high", Model: "m"}

	cases := []struct {
		name     string
		locator  fakeLocator
		source   *fakeSource
		step2    ports.ProposeResult
		paths    map[string]string
		readPath string
	}{
		{
			name:     "source_read_error",
			locator:  fakeLocator{filePath: "models/table_e.sql", serviceName: "svc"},
			source:   &fakeSource{err: fmt.Errorf("github 503")},
			step2:    ports.ProposeResult{ProposedSQL: "select customer_id from t", Confidence: "high", Model: "m"},
			paths:    map[string]string{"svc": "services/svc"},
			readPath: "services/svc/models/table_e.sql",
		},
		{
			name:    "unknown_location",
			locator: fakeLocator{filePath: "", serviceName: "svc"},
			source:  &fakeSource{err: fmt.Errorf("must not be called")},
			step2:   ports.ProposeResult{ProposedSQL: "select customer_id from t", Confidence: "high", Model: "m"},
			paths:   map[string]string{"svc": "services/svc"},
		},
		{
			name:    "unmapped_service",
			locator: fakeLocator{filePath: "models/table_e.sql", serviceName: "unknown-service"},
			source:  &fakeSource{content: "should not be called"},
			step2:   ports.ProposeResult{ProposedSQL: "select customer_id from t", Confidence: "high", Model: "m"},
			paths:   map[string]string{"svc": "services/svc"},
		},
		{
			name:     "unchanged_source",
			locator:  fakeLocator{filePath: "models/table_e.sql", serviceName: "svc"},
			source:   &fakeSource{content: originalSQL},
			step2:    ports.ProposeResult{ProposedSQL: originalSQL, Rationale: "no change", Confidence: "high", Model: "m"},
			paths:    map[string]string{"svc": "services/svc"},
			readPath: "services/svc/models/table_e.sql",
		},
		{
			name:     "low_confidence_source_fix",
			locator:  fakeLocator{filePath: "models/table_e.sql", serviceName: "svc"},
			source:   &fakeSource{content: originalSQL},
			step2:    ports.ProposeResult{ProposedSQL: "select fixed_id from t", Rationale: "maybe", Confidence: "low", Model: "m"},
			paths:    map[string]string{"svc": "services/svc"},
			readPath: "services/svc/models/table_e.sql",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := newFakeUoW()
			ev := fakeEvidence{vals: map[string]string{
				"s3://b/sql": "select custmer_id from t",
				"s3://b/log": "column does not exist",
			}}
			llm := fakeLLM{queue: []ports.ProposeResult{step1, tc.step2}, errs: []error{nil, nil}}
			art := &fakeArtifacts{}
			gw := &fakeGateway{imageTag: "tag-1"}
			d := deps(u, ev, &llm, art)
			d.Source = tc.source
			d.Locator = tc.locator
			d.ServiceRepoPaths = tc.paths
			d.Releases = gw
			tr := baseTrigger()
			tr.Nodes[0].FilePath = "" // no threaded path: the locator answers instead

			require.NoError(t, ProposeFix(context.Background(), d, tr))

			require.Len(t, u.pr.inserted, 1)
			p := u.pr.inserted[0]
			require.Equal(t, proposal.StatusFailed, p.Status)
			require.Equal(t, proposal.StatusFailed, p.NodeOutcomes["s.n"].Status)
			require.NotEmpty(t, p.NodeOutcomes["s.n"].Reason, "the operator must be told why nothing is being verified")
			require.False(t, p.SourceResolved)
			require.Empty(t, p.Edits)
			require.Empty(t, p.Verifications)
			require.Empty(t, gw.submitted, "no release slot is spent on a fix that changes no file")
			require.Len(t, art.written, 2, "only the candidate audit artifacts are written")
			require.Equal(t, tc.readPath, tc.source.readPath)
			require.Empty(t, p.Repo, "an attempt that resolved no source names no commit to open a PR against")
			require.Empty(t, p.CommitSHA)
			require.Empty(t, p.FilePath)
		})
	}
}

// TestProposeFix_SourceResolvedPersistsSourceLocation verifies that a verified
// attempt carries the three source-location fields a pull request is opened
// from: Repo, CommitSHA, and the full repository-relative path.
func TestProposeFix_SourceResolvedPersistsSourceLocation(t *testing.T) {
	u := newFakeUoW()
	ev := fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
		"s3://art/proposed-fix/r1/s.n/attempt-1.source.sql": "{{ config() }}\nselect customer_id from {{ ref('t') }}",
	}}
	llm := fakeLLM{
		queue: []ports.ProposeResult{
			{ProposedSQL: "select customer_id from t", Rationale: "typo fix", Confidence: "high", Model: "m"},
			{ProposedSQL: "{{ config() }}\nselect customer_id from {{ ref('t') }}", Rationale: "typo fix", Confidence: "high", Model: "m"},
		},
		errs: []error{nil, nil},
	}
	d := deps(u, ev, &llm, &fakeArtifacts{})
	d.Source = &fakeSource{content: "{{ config() }}\nselect custmer_id from {{ ref('t') }}"}
	d.Locator = fakeLocator{filePath: "models/orders_d.sql", serviceName: "service-3"}
	d.ServiceRepoPaths = map[string]string{"service-3": "services/service-3"}
	d.Releases = &fakeGateway{imageTag: "tag-1"}

	tr := baseTrigger()
	tr.Repo = "owner/continuo-demo"
	tr.CommitSHA = "abc123"
	tr.Nodes[0].FilePath = ""
	tr.Nodes[0].Service = ""

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	p := u.pr.inserted[0]
	require.True(t, p.SourceResolved)
	require.Equal(t, "owner/continuo-demo", p.Repo)
	require.Equal(t, "abc123", p.CommitSHA)
	require.Equal(t, "services/service-3/models/orders_d.sql", p.FilePath)
}

// TestRecord_NormalizesRepresentativeViews verifies that record derives the
// single-value views of an attempt's batched fields before the row is
// persisted: the representative node follows the resolved set, and the
// single-file scalars follow edits[0], so a proposal whose scalars disagree with
// its own edits cannot be stored that way. The mismatched input is a shape only
// a hand-built value can produce, never the driver's own assembly, but record
// must not persist it regardless.
func TestRecord_NormalizesRepresentativeViews(t *testing.T) {
	u := newFakeUoW()
	d := deps(u, fakeEvidence{}, nil, &fakeArtifacts{})
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.b", ErrorSignature: "sig-b"},
		{NodeID: "s.a", ErrorSignature: "sig-a"},
	}

	p := proposal.Proposal{
		Status:     proposal.StatusVerifying,
		Confidence: proposal.ConfidenceHigh,
		Rationale:  "rationale",
		Model:      "m",
		// Scalars deliberately disagree with edits[0].
		FilePath:       "stale/path.sql",
		ProposedSQLURI: "s3://stale/content",
		DiffURI:        "s3://stale/diff",
		Edits: []proposal.FileEdit{
			{Path: "services/svc/models/orders_d.sql", ContentURI: "s3://real/content", DiffURI: "s3://real/diff"},
		},
		Verifications: []proposal.Verification{{Service: "svc", Kind: ports.ShadowKindDbt, ShadowReleaseID: "shadow-r1-svc-a1"}},
	}

	require.NoError(t, record(context.Background(), d, tr, 1, p))

	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, "services/svc/models/orders_d.sql", got.FilePath)
	require.Equal(t, "s3://real/content", got.ProposedSQLURI)
	require.Equal(t, "s3://real/diff", got.DiffURI)
	require.Equal(t, []string{"s.a", "s.b"}, got.ResolvedNodeIDs,
		"an attempt that names no set addresses the trigger's whole failing set")
	require.Equal(t, "s.a", got.NodeID)
	require.Equal(t, "sig-a", got.ErrorSignature)
	require.Equal(t, "shadow-r1-svc-a1", got.ShadowReleaseID)
	require.Empty(t, u.ob.entries, "record announces nothing")
}

// TestEnqueue_CarriesTheResolvedSetAndEveryEdit pins the announcement the
// shadow-verification reconciler makes once a fix is verified: it names every
// node the attempt resolved and every file it changed, keyed on
// (release, attempt) so a repeated announcement collapses to one event.
func TestEnqueue_CarriesTheResolvedSetAndEveryEdit(t *testing.T) {
	u := newFakeUoW()
	p := proposal.Proposal{
		Source: "validation", ReleaseID: "r1", RemediationRound: 1, Attempt: 2,
		NodeID: "s.a", ResolvedNodeIDs: []string{"s.a", "s.b"}, ErrorSignature: "sig-a",
		ProposedSQLURI: "s3://real/content", DiffURI: "s3://real/diff",
		Rationale: "fixed", Confidence: proposal.ConfidenceHigh, Model: "m",
		Edits: []proposal.FileEdit{
			{Path: "services/svc/models/a.sql", ContentURI: "s3://real/content", DiffURI: "s3://real/diff", TargetNodeID: "s.a"},
			{Path: "services/svc/models/b.sql", ContentURI: "s3://b/content", DiffURI: "s3://b/diff", TargetNodeID: "s.b"},
		},
	}

	require.NoError(t, Enqueue(context.Background(), u, fakeClock{}, p, "s.root", true, uuid.Nil))

	require.Len(t, u.ob.entries, 1)
	var got event.RemediationProposed
	require.NoError(t, json.Unmarshal(u.ob.entries[0].Payload, &got))
	require.Equal(t, event.RemediationEventID("r1", 2).String(), got.EventID)
	require.Equal(t, "s.a", got.NodeID)
	require.Equal(t, []string{"s.a", "s.b"}, got.ResolvedNodeIDs)
	require.Equal(t, "s.root", got.SuspectedRootCauseNode)
	require.True(t, got.SourceResolved)
	require.Equal(t, []event.ProposedEdit{
		{Path: "services/svc/models/a.sql", ContentURI: "s3://real/content", DiffURI: "s3://real/diff", TargetNodeID: "s.a"},
		{Path: "services/svc/models/b.sql", ContentURI: "s3://b/content", DiffURI: "s3://b/diff", TargetNodeID: "s.b"},
	}, got.Edits)
	require.Nil(t, u.ob.entries[0].MessageProcessingID, "a write no inbound message drove records no provenance")
}

// TestServiceForPath_PicksTheNearestConfiguredRoot pins how an edit is routed
// to the service whose shadow release will run it: the longest configured root
// wins, a root at the repository itself catches what nothing else claims, and a
// path outside every root is refused rather than guessed at.
func TestServiceForPath_PicksTheNearestConfiguredRoot(t *testing.T) {
	paths := map[string]string{"svc": "services/svc", "nested": "services/svc/nested", "root": ""}

	service, prefix, ok := serviceForPath(paths, "services/svc/models/a.sql")
	require.True(t, ok)
	require.Equal(t, "svc", service)
	require.Equal(t, "services/svc", prefix)

	service, prefix, ok = serviceForPath(paths, "services/svc/nested/models/a.sql")
	require.True(t, ok)
	require.Equal(t, "nested", service)
	require.Equal(t, "services/svc/nested", prefix)

	service, _, ok = serviceForPath(paths, "elsewhere/a.sql")
	require.True(t, ok)
	require.Equal(t, "root", service, "a service rooted at the repository owns what nothing else claims")

	_, _, ok = serviceForPath(map[string]string{"svc": "services/svc"}, "elsewhere/a.sql")
	require.False(t, ok)
}
