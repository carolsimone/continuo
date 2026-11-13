package runner_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

const (
	workerOwner = "worker-1"
	releaseID   = "rel-1"
	repoName    = "org/repo"
	commitSHA   = "abc123"
	lease       = 5 * time.Minute

	flowFile     = "flows/rejected-release.yaml"
	flowRevision = "flowrev00001"
)

var (
	t0      = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	discard = slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx     = context.Background()
)

// testClock is a clock the test moves.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// fakeSource is the repository at the trigger's commit. fail is returned by
// the next ReadFile, once, and onRead runs on every read.
type fakeSource struct {
	mu     sync.Mutex
	files  map[string]string
	fail   error
	onRead func()
	// block makes every read wait until its context ends, as a hung call does.
	block atomic.Bool
}

func (s *fakeSource) ReadFile(ctx context.Context, _, _, path string) (string, error) {
	if s.block.Load() {
		<-ctx.Done()
		return "", ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onRead != nil {
		s.onRead()
	}
	if s.fail != nil {
		err := s.fail
		s.fail = nil
		return "", err
	}
	c, ok := s.files[path]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

func (s *fakeSource) ListDir(context.Context, string, string, string) ([]string, error) {
	return nil, ports.ErrSourceNotFound
}

// fakeEvidence reads the objects it holds, and every artifact the runner
// wrote, by its URI.
type fakeEvidence struct {
	objects   map[string]string
	artifacts *memArtifacts
}

func (e fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	if c, ok := e.objects[uri]; ok {
		return c, nil
	}
	if key, ok := strings.CutPrefix(uri, artifactsURIPrefix); ok {
		if c, ok := e.artifacts.get(key); ok {
			return c, nil
		}
	}
	return "", ports.ErrNotFound
}

type nopSanitizer struct{}

func (nopSanitizer) Sanitize(s string) string { return s }

type noPrecedents struct{}

func (noPrecedents) Precedents(context.Context, ports.PrecedentQuery) ([]prompt.Precedent, error) {
	return nil, nil
}

// noBundle is a code bundle that holds only the nodes in nodes. onRead runs
// on every read.
type noBundle struct {
	mu     sync.Mutex
	reads  int
	nodes  map[string]ports.CandidateSource
	onRead func(n int)
}

func (b *noBundle) NodeSource(_ context.Context, _, nodeID, _ string) (ports.CandidateSource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.onRead != nil {
		b.onRead(b.reads)
	}
	if src, ok := b.nodes[nodeID]; ok {
		return src, nil
	}
	return ports.CandidateSource{}, ports.ErrNotFound
}

// noVersions is a graph with no running version of any node.
type noVersions struct{}

func (noVersions) CurrentVersion(context.Context, string) (ports.CurrentVersion, bool, error) {
	return ports.CurrentVersion{}, false, nil
}

// fakeReleases reads the failing release's image tag of a service as
// "tag-<service>".
type fakeReleases struct{}

func (fakeReleases) ImageTag(_ context.Context, _, service string) (string, error) {
	return "tag-" + service, nil
}

func (fakeReleases) FailingNodes(context.Context, string) (map[string]string, error) {
	return nil, nil
}

// fakePipeline records every verification run submitted. submitErr is
// returned by the next Submit, once. onSubmit runs after each run it records.
// status answers every status read, queued when it is nil, and onStatus runs
// before each read with the read's context. A read whose context ended by
// then returns the context's error, as a call to release-controller does.
type fakePipeline struct {
	mu        sync.Mutex
	submitted []ports.VerificationRequest
	submitErr error
	onSubmit  func()
	status    func(runID string) (ports.VerificationStatus, error)
	onStatus  func(ctx context.Context)
	reads     int
}

func (p *fakePipeline) Submit(_ context.Context, r ports.VerificationRequest) error {
	p.mu.Lock()
	if p.submitErr != nil {
		err := p.submitErr
		p.submitErr = nil
		p.mu.Unlock()
		return err
	}
	p.submitted = append(p.submitted, r)
	onSubmit := p.onSubmit
	p.mu.Unlock()
	if onSubmit != nil {
		onSubmit()
	}
	return nil
}

func (p *fakePipeline) Status(ctx context.Context, runID string) (ports.VerificationStatus, error) {
	p.mu.Lock()
	status, onStatus := p.status, p.onStatus
	p.reads++
	p.mu.Unlock()
	if onStatus != nil {
		onStatus(ctx)
	}
	if err := ctx.Err(); err != nil {
		return ports.VerificationStatus{}, err
	}
	if status == nil {
		return ports.VerificationStatus{Phase: proposal.PhaseQueued}, nil
	}
	return status(runID)
}

// answer makes every run read as phase, with nodeErrors when it failed and
// activated at t0 once it left the queue.
func (p *fakePipeline) answer(phase proposal.Phase, nodeErrors map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = func(string) (ports.VerificationStatus, error) {
		st := ports.VerificationStatus{Phase: phase, NodeErrors: nodeErrors}
		if phase != proposal.PhaseQueued {
			st.ActivatedAt = t0
		}
		return st, nil
	}
}

func (p *fakePipeline) runs() []ports.VerificationRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ports.VerificationRequest(nil), p.submitted...)
}

// artifactsURIPrefix is the URI prefix of every artifact memArtifacts stores.
const artifactsURIPrefix = "s3://artifacts/"

// memArtifacts stores every artifact written, by key.
type memArtifacts struct {
	mu      sync.Mutex
	objects map[string]string
}

func (a *memArtifacts) Write(_ context.Context, key, body, _ string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.objects == nil {
		a.objects = map[string]string{}
	}
	a.objects[key] = body
	return artifactsURIPrefix + key, nil
}

func (a *memArtifacts) get(key string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.objects[key]
	return v, ok
}

// scriptedLLM answers every request with answer, records the requests, and
// runs onCall after each call with the number of calls so far.
type scriptedLLM struct {
	mu       sync.Mutex
	requests []prompt.ProposeRequest
	answer   func(req prompt.ProposeRequest) ports.ProposeResult
	onCall   func(n int)
	// err, when set, is returned by every call.
	err error
}

func (l *scriptedLLM) Propose(_ context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	l.mu.Lock()
	l.requests = append(l.requests, req)
	n := len(l.requests)
	answer, onCall, err := l.answer, l.onCall, l.err
	l.mu.Unlock()
	if onCall != nil {
		onCall(n)
	}
	if err != nil {
		return ports.ProposeResult{}, err
	}
	return answer(req), nil
}

func (l *scriptedLLM) calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.requests)
}

func (l *scriptedLLM) lastRequest() prompt.ProposeRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.requests[len(l.requests)-1]
}

// memCache is the LLM response cache, in memory.
type memCache struct {
	mu sync.Mutex
	m  map[string]ports.ProposeResult
}

func (c *memCache) Get(_ context.Context, key string) (ports.ProposeResult, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.m[key]
	return r, ok, nil
}

func (c *memCache) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.m[key]
	return ok
}

func (c *memCache) Put(_ context.Context, key string, r ports.ProposeResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]ports.ProposeResult{}
	}
	c.m[key] = r
	return nil
}

// fixAnswer is the model's answer to a compile request: the one shown file,
// corrected, with high confidence.
func fixAnswer(prompt.ProposeRequest) ports.ProposeResult {
	return ports.ProposeResult{
		ProposedContent: "select 2",
		Rationale:       "fixed the select",
		Confidence:      "high",
		Model:           "claude-test",
		Usage:           ports.TokenUsage{Input: 100, Output: 20},
	}
}

func baseName(stream string) string {
	name, _, _ := strings.Cut(stream, ":")
	return name
}

func refs(names ...string) []workflow.StepRef {
	out := make([]workflow.StepRef, 0, len(names))
	for _, n := range names {
		out = append(out, workflow.StepRef{Name: workflow.StepName(n)})
	}
	return out
}

// testFiles is a flow with four of the built-in types: a compile type, the
// validation type, and the two upstream types.
func testFiles(tries int) workflow.Set {
	flow := workflow.Flow{
		Version:        workflow.SupportedVersion,
		Name:           "rejected-release",
		Trigger:        baseName(streams.RemediationRequestedV2),
		Types:          []string{"dbt-compile", "dbt-validation", "shared-upstream", "cross-service-upstream"},
		AcrossClusters: refs("edits_map_to_a_service", "no_path_edited_twice", "one_manifest_kind"),
		Gate:           refs("verification_run"),
		Tries:          tries,
		OnPass:         workflow.Output{Action: workflow.OutputPullRequest, Emits: baseName(streams.RemediationProposedV1)},
		OnGiveUp:       workflow.Output{Action: workflow.OutputHandOver},
		Revision:       flowRevision,
		File:           flowFile,
	}
	typ := func(name string, m workflow.Match, context []string, promptStep string, preflight []string) workflow.Type {
		return workflow.Type{
			Version:   workflow.SupportedVersion,
			Name:      name,
			Match:     m,
			Context:   refs(context...),
			Agent:     workflow.Agent{Prompt: workflow.StepRef{Name: workflow.StepName(promptStep)}, Tools: []workflow.StepRef{}, Rounds: 1},
			Preflight: refs(preflight...),
			Revision:  "rev-" + name,
			File:      "types/" + name + ".yaml",
		}
	}
	types := []workflow.Type{
		typ("dbt-compile", workflow.Match{Cluster: workflow.ClusterSingle, Source: "compile"},
			[]string{"failing_node_source", "dbt_log", "failure_precedents"}, "dbt-compile",
			[]string{"target_file_was_shown", "content_changed", "confidence_not_low"}),
		typ("dbt-validation", workflow.Match{Cluster: workflow.ClusterSingle, Source: "validation", NodeTypes: []string{"dbt-model", "dbt-seed", "dbt-snapshot", "none"}},
			[]string{"candidate_sql", "dbt_log", "code_bundle_source", "own_change_diff", "upstream_changes", "changed_ancestor_diffs", "failure_precedents"},
			"dbt-validation", []string{"content_changed"}),
		typ("shared-upstream", workflow.Match{Cluster: workflow.ClusterSharedUpstream},
			[]string{"upstream_target", "failure_precedents"}, "upstream", []string{"content_changed", "confidence_not_low"}),
		typ("cross-service-upstream", workflow.Match{Cluster: workflow.ClusterCrossService},
			[]string{"upstream_target", "failure_precedents"}, "upstream", []string{"content_changed", "confidence_not_low"}),
	}
	set := workflow.Set{Flows: map[string]workflow.Flow{flow.Name: flow}, Types: map[string]workflow.Type{}}
	for _, t := range types {
		set.Types[t.Name] = t
	}
	return set
}

// harness is one runner over a memory store, with fakes for every port the
// test types' steps call.
type harness struct {
	t           *testing.T
	store       *memory.Store
	clock       *testClock
	source      *fakeSource
	evidence    fakeEvidence
	bundle      *noBundle
	artifacts   *memArtifacts
	pipeline    *fakePipeline
	llm         *scriptedLLM
	cache       *memCache
	files       workflow.Set
	maintenance bool
	stepTimeout time.Duration
	collab      runner.Collaborators
	runner      *runner.Runner

	// afterCommit runs after every unit of work of the runner commits.
	afterCommit func()
	// blockBeginUnderADeadline makes a transaction that begins in a context with
	// a deadline wait until the context ends, as a database that hangs does.
	blockBeginUnderADeadline atomic.Bool
	// phaseWrites counts the runner's UpdateVerificationPhase calls.
	phaseWrites atomic.Int64
}

// verificationTimeout is how long the gate lets a verification run take.
const verificationTimeout = 20 * time.Minute

// hookedUoW is a memory unit of work that runs the harness's afterCommit hook
// after it commits and counts the phase writes of its proposal repository.
type hookedUoW struct {
	uow.UnitOfWork
	h *harness
}

func (u hookedUoW) Begin(ctx context.Context) error {
	if _, ok := ctx.Deadline(); ok && u.h.blockBeginUnderADeadline.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	return u.UnitOfWork.Begin(ctx)
}

func (u hookedUoW) Commit() error {
	if err := u.UnitOfWork.Commit(); err != nil {
		return err
	}
	if u.h.afterCommit != nil {
		u.h.afterCommit()
	}
	return nil
}

func (u hookedUoW) ProposalRepo() repository.ProposalRepository {
	return phaseCounter{ProposalRepository: u.UnitOfWork.ProposalRepo(), h: u.h}
}

type phaseCounter struct {
	repository.ProposalRepository
	h *harness
}

func (p phaseCounter) UpdateVerificationPhase(ctx context.Context, id, runID string, phase proposal.Phase, activatedAt *time.Time) error {
	p.h.phaseWrites.Add(1)
	return p.ProposalRepository.UpdateVerificationPhase(ctx, id, runID, phase, activatedAt)
}

type option func(*harness)

func withMaintenance() option { return func(h *harness) { h.maintenance = true } }

func withStepTimeout(d time.Duration) option { return func(h *harness) { h.stepTimeout = d } }

func withTries(n int) option {
	return func(h *harness) { h.files = testFiles(n) }
}

// withoutCrossClusterChecks gives the flow no cross-cluster check, so the gate
// is the first to see the try's drafts together.
func withoutCrossClusterChecks() option {
	return func(h *harness) {
		flow := h.files.Flows["rejected-release"]
		flow.AcrossClusters = nil
		h.files.Flows[flow.Name] = flow
	}
}

// withFlowContext gives the flow the context step dbt_log, which the
// dbt-compile type then no longer lists itself.
func withFlowContext() option {
	return func(h *harness) {
		flow := h.files.Flows["rejected-release"]
		flow.Context = refs("dbt_log")
		h.files.Flows[flow.Name] = flow
		typ := h.files.Types["dbt-compile"]
		typ.Context = refs("failing_node_source", "failure_precedents")
		h.files.Types[typ.Name] = typ
	}
}

func newHarness(t *testing.T, opts ...option) *harness {
	t.Helper()
	artifacts := &memArtifacts{}
	h := &harness{
		t:     t,
		store: memory.NewStore(),
		clock: &testClock{now: t0},
		source: &fakeSource{files: map[string]string{
			"services/svc-a/models/orders.sql": "select 1",
			"services/svc-b/models/orders.sql": "select 1",
		}},
		evidence:  fakeEvidence{objects: map[string]string{}, artifacts: artifacts},
		bundle:    &noBundle{},
		artifacts: artifacts,
		pipeline:  &fakePipeline{},
		llm:       &scriptedLLM{answer: fixAnswer},
		cache:     &memCache{},
		files:     testFiles(3),
	}
	for _, o := range opts {
		o(h)
	}
	h.collab = runner.Collaborators{
		Source:    h.source,
		Evidence:  h.evidence,
		Sanitizer: nopSanitizer{},
		Artifacts: h.artifacts,
		Logger:    discard,
		ServiceRepoPaths: map[string]string{
			"svc-a": "services/svc-a",
			"svc-b": "services/svc-b",
			"core":  "services/core",
		},
		Precedents:          noPrecedents{},
		CandidateSource:     h.bundle,
		Versions:            noVersions{},
		Releases:            fakeReleases{},
		Pipeline:            h.pipeline,
		Clock:               h.clock,
		VerificationTimeout: verificationTimeout,
	}
	_, err := runner.Validate(h.files, runner.NewRegistry(h.collab))
	require.NoError(t, err)
	h.runner = h.runnerOver(h.files)
	return h
}

// runnerOver is a runner over the harness's store and fakes that runs files.
func (h *harness) runnerOver(files workflow.Set) *runner.Runner {
	return h.runnerOwnedBy(files, workerOwner)
}

// runnerOwnedBy is a runner over the harness's store and fakes that runs files
// and holds its leases as owner.
func (h *harness) runnerOwnedBy(files workflow.Set, owner string) *runner.Runner {
	return runner.New(runner.Deps{
		NewUoW:              func() uow.UnitOfWork { return hookedUoW{UnitOfWork: memory.NewUnitOfWork(h.store), h: h} },
		Registry:            runner.NewRegistry(h.collab),
		Files:               files,
		LLM:                 llmcache.New(h.llm, h.cache, "test-model", discard),
		Collab:              h.collab,
		Clock:               h.clock,
		Logger:              discard,
		Maintenance:         h.maintenance,
		Owner:               owner,
		VerificationTimeout: verificationTimeout,
		StepTimeout:         h.stepTimeout,
	})
}

func compileNode(id string) remediation.TriggerNode {
	return remediation.TriggerNode{
		NodeID:         id,
		ErrorSignature: "sig-" + id,
		Category:       "compile",
		Reason:         "syntax",
		ErrorExcerpt:   "syntax error near select",
		FilePath:       "models/orders.sql",
	}
}

func trigger(source string, nodes ...remediation.TriggerNode) remediation.Trigger {
	return remediation.Trigger{
		TenantID:  "default",
		Source:    source,
		ReleaseID: releaseID,
		Round:     1,
		Repo:      repoName,
		CommitSHA: commitSHA,
		Nodes:     nodes,
	}
}

// payloadOf is trig as a remediation.requested:v2 payload, in the wire field
// names.
func payloadOf(t *testing.T, trig remediation.Trigger) []byte {
	t.Helper()
	nodes := make([]map[string]any, 0, len(trig.Nodes))
	for _, n := range trig.Nodes {
		node := map[string]any{
			"node_id": n.NodeID, "relation_id": n.RelationID, "category": n.Category,
			"error_signature": n.ErrorSignature, "reason": n.Reason, "error_excerpt": n.ErrorExcerpt,
			"dbt_log_uri": n.DBTLogURI, "candidate_artifact_uri": n.CandidateArtifactURI,
			"file_path": n.FilePath, "service": n.Service, "node_type": n.NodeType,
			"other_service": n.OtherService, "other_file_path": n.OtherFilePath,
		}
		if len(n.ChangedAncestors) > 0 {
			ancestors := make([]map[string]any, 0, len(n.ChangedAncestors))
			for _, a := range n.ChangedAncestors {
				ancestors = append(ancestors, map[string]any{"node_id": a.NodeID, "file_path": a.FilePath, "service": a.Service, "depth": a.Depth})
			}
			node["changed_ancestors"] = ancestors
		}
		nodes = append(nodes, node)
	}
	raw, err := json.Marshal(map[string]any{
		"source": trig.Source, "release_id": trig.ReleaseID, "remediation_round": trig.Round,
		"repo": trig.Repo, "commit_sha": trig.CommitSHA, "code_bundle_uri": trig.CodeBundleURI,
		"nodes": nodes,
	})
	require.NoError(t, err)
	return raw
}

func (h *harness) accepted(trig remediation.Trigger, messageID string) runner.Accepted {
	return runner.Accepted{
		Trigger:   trig,
		Payload:   payloadOf(h.t, trig),
		MessageID: messageID,
		Fields:    map[string]string{"tenant_id": trig.TenantID},
	}
}

// accept accepts trig and returns the remediation it created, claimed by the
// runner's owner.
func (h *harness) accept(trig remediation.Trigger) uuid.UUID {
	h.t.Helper()
	require.NoError(h.t, h.runner.Accept(ctx, h.accepted(trig, "1-0")))
	rems := h.store.Remediations()
	require.Len(h.t, rems, 1)
	claimed, err := h.store.RemediationRepository().Claim(ctx, rems[0].ID, workerOwner, h.clock.Now(), lease)
	require.NoError(h.t, err)
	require.True(h.t, claimed)
	return rems[0].ID
}

// legacyAttempt records a terminal attempt today's driver wrote for the
// release.
func (h *harness) legacyAttempt(attempt int, status proposal.Status) {
	h.t.Helper()
	h.legacyAttemptInRound(1, attempt, status)
}

// saveStep writes a journal row as it stands.
func (h *harness) saveStep(row remediation.StepRun) {
	h.t.Helper()
	u := memory.NewUnitOfWork(h.store)
	require.NoError(h.t, u.Begin(ctx))
	require.NoError(h.t, u.RemediationRepo().SaveStep(ctx, row))
	require.NoError(h.t, u.Commit())
}

// legacyAttemptInRound records a terminal attempt today's driver wrote for the
// release in one round.
func (h *harness) legacyAttemptInRound(round, attempt int, status proposal.Status) {
	h.t.Helper()
	u := memory.NewUnitOfWork(h.store)
	require.NoError(h.t, u.Begin(ctx))
	require.NoError(h.t, u.ProposalRepo().Upsert(ctx, proposal.Proposal{
		Source: "compile", ReleaseID: releaseID, RemediationRound: round, Attempt: attempt, Status: status,
		ResolvedNodeIDs: []string{"svc-a"}, CreatedAt: t0,
	}))
	require.NoError(h.t, u.Commit())
}

func (h *harness) proposals() []memory.StoredProposal { return h.store.Proposals() }

func (h *harness) remediation(id uuid.UUID) remediation.Remediation {
	h.t.Helper()
	for _, r := range h.store.Remediations() {
		if r.ID == id {
			return r
		}
	}
	h.t.Fatalf("no remediation %s", id)
	return remediation.Remediation{}
}

// stepRows returns the journal rows of one cluster of one try, by step name.
func (h *harness) stepRows(id uuid.UUID, try, cluster int) map[string][]remediation.StepRun {
	out := map[string][]remediation.StepRun{}
	for _, s := range h.store.Steps(id) {
		if s.Try == try && s.Cluster == cluster {
			out[s.Step] = append(out[s.Step], s)
		}
	}
	return out
}

func (h *harness) cluster(id uuid.UUID, try, n int) remediation.Cluster {
	h.t.Helper()
	for _, c := range h.store.Clusters(id) {
		if c.Try == try && c.Cluster == n {
			return c
		}
	}
	h.t.Fatalf("no cluster %d in try %d", n, try)
	return remediation.Cluster{}
}

// reclaim gives the lease on the remediation to the runner's owner again from
// now, as the worker and the poller claim it before they act.
func (h *harness) reclaim(id uuid.UUID) {
	h.t.Helper()
	repo := h.store.RemediationRepository()
	require.NoError(h.t, repo.ReleaseLease(ctx, id, workerOwner))
	_, err := repo.Claim(ctx, id, workerOwner, h.clock.Now(), lease)
	require.NoError(h.t, err)
}

// drive claims the remediation and drives it, as the worker does.
func (h *harness) drive(id uuid.UUID) error {
	h.t.Helper()
	h.reclaim(id)
	return h.runner.Drive(ctx, id)
}

// poll claims the remediation and polls its gate with the snapshot the poller
// would have listed, as the gate poller does.
func (h *harness) poll(id uuid.UUID) error {
	h.t.Helper()
	h.reclaim(id)
	snap, err := h.store.RemediationRepository().Get(ctx, id)
	require.NoError(h.t, err)
	return h.runner.PollGate(ctx, snap)
}

// attempt returns the proposal row of one attempt of the release.
func (h *harness) attempt(n int) memory.StoredProposal {
	h.t.Helper()
	for _, p := range h.proposals() {
		if p.Proposal.Attempt == n {
			return p
		}
	}
	h.t.Fatalf("no proposal row for attempt %d", n)
	return memory.StoredProposal{}
}

// try returns one try of the remediation.
func (h *harness) try(id uuid.UUID, n int) remediation.Try {
	h.t.Helper()
	for _, t := range h.store.Tries(id) {
		if t.Try == n {
			return t
		}
	}
	h.t.Fatalf("no try %d", n)
	return remediation.Try{}
}

// flowRows returns the flow-level journal rows of one try, by step name.
func (h *harness) flowRows(id uuid.UUID, try int) map[string][]remediation.StepRun {
	return h.stepRows(id, try, 0)
}

// proposedEvents is every outbox entry on remediation.proposed:v1.
func (h *harness) proposedEvents() []outbox.Entry {
	var out []outbox.Entry
	for _, e := range h.store.OutboxEntries() {
		if e.StreamName == streams.RemediationProposedV1 {
			out = append(out, e)
		}
	}
	return out
}
