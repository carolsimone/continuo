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
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
	"github.com/carolsimone/continuo/pkg/domain/model"
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

// noUpstream is a graph that knows no change upstream of any node.
type noUpstream struct{}

func (noUpstream) UpstreamChanges(context.Context, string) ([]prompt.UpstreamChange, error) {
	return nil, nil
}

// contractFiles searches and reads a checkout's contract yaml as the component
// does.
var contractFiles = repofs.NewLocator(discard)

type noPrecedents struct{}

func (noPrecedents) Precedents(context.Context, ports.PrecedentQuery) ([]prompt.Precedent, error) {
	return nil, nil
}

// noBundle is a code bundle that holds only the nodes in nodes and records
// the owners in owners. onRead runs on every source read, ownerReads counts the
// owners reads, and every read answers err while it is set.
type noBundle struct {
	mu         sync.Mutex
	reads      int
	ownerReads int
	nodes      map[string]ports.CandidateSource
	owners     map[string]string
	onRead     func(n int)
	err        error
}

func (b *noBundle) NodeOwners(_ context.Context, _, _ string, nodeIDs []string) (map[string]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ownerReads++
	if b.err != nil {
		return nil, b.err
	}
	out := map[string]string{}
	for _, id := range nodeIDs {
		if o, ok := b.owners[id]; ok {
			out[id] = o
		}
	}
	return out, nil
}

// ownerReadCount is how many owners reads the bundle answered.
func (b *noBundle) ownerReadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ownerReads
}

func (b *noBundle) NodeSource(_ context.Context, _, nodeID, _ string) (ports.CandidateSource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.onRead != nil {
		b.onRead(b.reads)
	}
	if b.err != nil {
		return ports.CandidateSource{}, b.err
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

// harnessPackages is the package list fakeImages answers.
var harnessPackages = map[string]string{"pyarrow": "26.0.0"}

// fakeImages answers harnessPackages, or err, for every release and project,
// and counts the reads.
type fakeImages struct {
	mu    sync.Mutex
	err   error
	reads int
}

func (f *fakeImages) ImagePackages(context.Context, string, string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	return harnessPackages, nil
}

func (f *fakeImages) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// noContracts is a catalog in which no node has a contract.
type noContracts struct{}

func (noContracts) CurrentContract(context.Context, string) (string, error) { return "", nil }

// fakeReleases reads the failing release's image tag of a project as
// "tag-<project>".
type fakeReleases struct{}

func (fakeReleases) ImageTag(_ context.Context, _, project string) (string, error) {
	return "tag-" + project, nil
}

func (fakeReleases) FailingNodes(context.Context, string) (map[string]string, error) {
	return nil, nil
}

func (fakeReleases) ContractBundle(context.Context, string) (string, string, error) {
	return "", "", nil
}

// fakeReleaseChecks records every release check submitted. submitErr is
// returned by the next Submit, once. onSubmit runs after each check it records.
// status answers every status read, queued when it is nil, and onStatus runs
// before each read with the read's context. A read whose context ended by
// then returns the context's error, as a call to release-controller does.
type fakeReleaseChecks struct {
	mu        sync.Mutex
	submitted []ports.ReleaseCheckRequest
	submitErr error
	onSubmit  func()
	status    func(checkID string) (ports.ReleaseCheckStatus, error)
	onStatus  func(ctx context.Context)
	reads     int
}

func (p *fakeReleaseChecks) Submit(_ context.Context, r ports.ReleaseCheckRequest) error {
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

func (p *fakeReleaseChecks) Status(ctx context.Context, checkID string) (ports.ReleaseCheckStatus, error) {
	p.mu.Lock()
	status, onStatus := p.status, p.onStatus
	p.reads++
	p.mu.Unlock()
	if onStatus != nil {
		onStatus(ctx)
	}
	if err := ctx.Err(); err != nil {
		return ports.ReleaseCheckStatus{}, err
	}
	if status == nil {
		return ports.ReleaseCheckStatus{Phase: fix.PhaseQueued}, nil
	}
	return status(checkID)
}

// answer makes every check read as phase, with nodeErrors when it failed and
// activated at t0 once it left the queue.
func (p *fakeReleaseChecks) answer(phase fix.Phase, nodeErrors map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = func(string) (ports.ReleaseCheckStatus, error) {
		st := ports.ReleaseCheckStatus{Phase: phase, NodeErrors: nodeErrors}
		if phase != fix.PhaseQueued {
			st.ActivatedAt = t0
		}
		return st, nil
	}
}

// statusReads is how many status reads the release checks answered.
func (p *fakeReleaseChecks) statusReads() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reads
}

func (p *fakeReleaseChecks) requests() []ports.ReleaseCheckRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ports.ReleaseCheckRequest(nil), p.submitted...)
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
		Trigger:        baseName(streams.HealTriggeredV1),
		Types:          []string{"dbt-compile", "dbt-validation", "shared-upstream", "cross-project-upstream"},
		AcrossClusters: refs("edits_map_to_a_project", "no_path_edited_twice", "one_manifest_kind"),
		Gate:           refs("release_check"),
		Tries:          tries,
		OnPass:         workflow.Output{Action: workflow.OutputPullRequest, Emits: baseName(streams.FixProposedV1)},
		OnGiveUp:       workflow.Output{Action: workflow.OutputHandOver, Emits: baseName(streams.RemediationHandedOverV1)},
		ModelMaySee:    []workflow.DataClass{workflow.ClassCode, workflow.ClassLogs},
		Approver:       workflow.StepRef{Name: "node_owner"},
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
			"dbt-validation", []string{"content_changed", "confidence_not_low"}),
		typ("shared-upstream", workflow.Match{Cluster: workflow.ClusterSharedUpstream},
			[]string{"upstream_target", "failure_precedents"}, "upstream", []string{"content_changed", "confidence_not_low"}),
		typ("cross-project-upstream", workflow.Match{Cluster: workflow.ClusterCrossProject},
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
	checks      *fakeReleaseChecks
	llm         *scriptedLLM
	cache       *memCache
	files       workflow.Set
	maintenance bool
	stepTimeout time.Duration
	// archive and packager are the repository checkout and the contract
	// packaging tool the contract steps read, nil unless a test sets them.
	archive  ports.RepoArchive
	packager ports.ContractPackager
	// images answers the image package list of every release and project.
	images *fakeImages
	collab runner.Collaborators
	runner *runner.Runner
	// replaced are the steps that stand in for the registered steps of their
	// names in every runner the harness builds.
	replaced []any

	// afterCommit runs after every unit of work of the runner commits.
	afterCommit func()
	// blockBeginUnderADeadline makes a transaction that begins in a context with
	// a deadline wait until the context ends, as a database that hangs does.
	blockBeginUnderADeadline atomic.Bool
	// phaseWrites counts the runner's UpdateReleaseCheckPhase calls.
	phaseWrites atomic.Int64
	// metrics is what every runner the harness builds counts on, nil unless a
	// test sets it.
	metrics ports.RemediationMetrics
}

// releaseCheckTimeout is how long the gate lets a release check take.
const releaseCheckTimeout = 20 * time.Minute

// hookedUoW is a memory unit of work that runs the harness's afterCommit hook
// after it commits and counts the phase writes of its fix repository.
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

func (u hookedUoW) FixRepo() repository.FixRepository {
	return phaseCounter{FixRepository: u.UnitOfWork.FixRepo(), h: u.h}
}

type phaseCounter struct {
	repository.FixRepository
	h *harness
}

func (p phaseCounter) UpdateReleaseCheckPhase(ctx context.Context, id, checkID string, phase fix.Phase, activatedAt *time.Time) error {
	p.h.phaseWrites.Add(1)
	return p.FixRepository.UpdateReleaseCheckPhase(ctx, id, checkID, phase, activatedAt)
}

type option func(*harness)

func withMaintenance() option { return func(h *harness) { h.maintenance = true } }

func withStepTimeout(d time.Duration) option { return func(h *harness) { h.stepTimeout = d } }

// withMetrics makes every runner, worker and sweeper of the harness count on m.
func withMetrics(m ports.RemediationMetrics) option { return func(h *harness) { h.metrics = m } }

func withTries(n int) option {
	return func(h *harness) { h.files = testFiles(n) }
}

// withRounds gives every type of the harness rounds pre-flight rounds.
func withRounds(rounds int) option {
	return func(h *harness) {
		for name, typ := range h.files.Types {
			typ.Agent.Rounds = rounds
			h.files.Types[name] = typ
		}
	}
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

// withPreflight gives the type typeName the pre-flight checks checks.
func withPreflight(typeName string, checks ...string) option {
	return func(h *harness) {
		typ := h.files.Types[typeName]
		typ.Preflight = refs(checks...)
		h.files.Types[typeName] = typ
	}
}

// withBuiltinFiles runs the flows and types built into the binary.
func withBuiltinFiles() option {
	return func(h *harness) {
		set, err := workflowfile.Load(workflows.Builtin)
		require.NoError(h.t, err)
		h.files = set
	}
}

// withPreviousTries gives the flow the context step previous_tries, as the
// built-in flow lists it.
func withPreviousTries() option {
	return func(h *harness) {
		flow := h.files.Flows["rejected-release"]
		flow.Context = refs("previous_tries")
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
		checks:    &fakeReleaseChecks{},
		llm:       &scriptedLLM{answer: fixAnswer},
		cache:     &memCache{},
		images:    &fakeImages{},
		files:     testFiles(3),
	}
	for _, o := range opts {
		o(h)
	}
	h.collab = runner.Collaborators{
		Source:    h.source,
		Evidence:  h.evidence,
		Artifacts: h.artifacts,
		Logger:    discard,
		ProjectRepoPaths: map[string]string{
			"svc-a": "services/svc-a",
			"svc-b": "services/svc-b",
			"core":  "services/core",
		},
		Precedents:          noPrecedents{},
		Upstream:            noUpstream{},
		PriorFixes:          h.store.FixRepository(),
		Tries:               h.store.RemediationRepository(),
		CandidateSource:     h.bundle,
		Versions:            noVersions{},
		Releases:            fakeReleases{},
		Archive:             h.archive,
		Patcher:             repofs.CheckoutPatcher{},
		ContractLocator:     contractFiles,
		ContractInspector:   contractFiles,
		Packager:            h.packager,
		ImagePackages:       h.images,
		CurrentContracts:    noContracts{},
		ReleaseChecks:       h.checks,
		Clock:               h.clock,
		ReleaseCheckTimeout: releaseCheckTimeout,
	}
	_, err := runner.Validate(h.files, runner.NewRegistry(h.collab))
	require.NoError(t, err)
	h.runner = h.runnerOver(h.files)
	return h
}

// registry is the registry built from the harness's fakes, with each replaced
// step in place of the registered step of its name.
func (h *harness) registry() *runner.Registry {
	reg := runner.NewRegistry(h.collab)
	for _, s := range h.replaced {
		reg.ReplaceStep(s)
	}
	return reg
}

// replaceStep puts s in place of the registered step of its name, in the
// harness's runner and in every runner it builds after.
func (h *harness) replaceStep(s any) {
	h.replaced = append(h.replaced, s)
	h.runner = h.runnerOver(h.files)
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
		Registry:            h.registry(),
		Files:               files,
		LLM:                 llmcache.New(h.llm, h.cache, "test-model", discard),
		Collab:              h.collab,
		Clock:               h.clock,
		Logger:              discard,
		Maintenance:         h.maintenance,
		Owner:               owner,
		ReleaseCheckTimeout: releaseCheckTimeout,
		StepTimeout:         h.stepTimeout,
		Metrics:             h.metrics,
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

// unfixable is n marked unfixable by the classifier, for reason.
func unfixable(n remediation.TriggerNode, reason model.HandoverReason, detail string) remediation.TriggerNode {
	n.Unfixable = &remediation.Unfixable{Reason: reason, Detail: detail}
	return n
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

// payloadOf is trig as a heal.triggered:v1 payload, in the wire field
// names.
func payloadOf(t *testing.T, trig remediation.Trigger) []byte {
	t.Helper()
	nodes := make([]map[string]any, 0, len(trig.Nodes))
	for _, n := range trig.Nodes {
		node := map[string]any{
			"node_id": n.NodeID, "relation_id": n.RelationID, "category": n.Category,
			"error_signature": n.ErrorSignature, "reason": n.Reason, "error_excerpt": n.ErrorExcerpt,
			"dbt_log_uri": n.DBTLogURI, "candidate_artifact_uri": n.CandidateArtifactURI,
			"file_path": n.FilePath, "project": n.Project, "node_type": n.NodeType,
			"other_project": n.OtherProject, "other_file_path": n.OtherFilePath,
		}
		if n.Unfixable != nil {
			node["unfixable"] = map[string]any{"reason": string(n.Unfixable.Reason), "detail": n.Unfixable.Detail}
		}
		if len(n.ChangedAncestors) > 0 {
			ancestors := make([]map[string]any, 0, len(n.ChangedAncestors))
			for _, a := range n.ChangedAncestors {
				ancestors = append(ancestors, map[string]any{"node_id": a.NodeID, "file_path": a.FilePath, "project": a.Project, "depth": a.Depth})
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

// tryWithNoRemediation records a terminal fix row that belongs to no
// remediation, for the release.
func (h *harness) tryWithNoRemediation(try int, status fix.Status) {
	h.t.Helper()
	h.tryWithNoRemediationInRound(1, try, status)
}

// saveStep writes a journal row as it stands.
func (h *harness) saveStep(row remediation.StepRun) {
	h.t.Helper()
	u := memory.NewUnitOfWork(h.store)
	require.NoError(h.t, u.Begin(ctx))
	require.NoError(h.t, u.RemediationRepo().SaveStep(ctx, row))
	require.NoError(h.t, u.Commit())
}

// tryWithNoRemediationInRound records a terminal fix row that belongs to
// no remediation, for the release in one round.
func (h *harness) tryWithNoRemediationInRound(round, try int, status fix.Status) {
	h.t.Helper()
	u := memory.NewUnitOfWork(h.store)
	require.NoError(h.t, u.Begin(ctx))
	require.NoError(h.t, u.FixRepo().Upsert(ctx, fix.Fix{
		Source: "compile", ReleaseID: releaseID, RemediationRound: round, Try: try, Status: status,
		ResolvedNodeIDs: []string{"svc-a"}, CreatedAt: t0,
	}))
	require.NoError(h.t, u.Commit())
}

func (h *harness) fixes() []memory.StoredFix { return h.store.Fixes() }

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

// fixRow returns the fix row of try n of the release's round 1; n is
// fix.HandOverTry for the round's hand-over row.
func (h *harness) fixRow(n int) memory.StoredFix {
	h.t.Helper()
	return h.fixRowIn(1, n)
}

// fixRowIn returns the fix row of try n of the release's round.
func (h *harness) fixRowIn(round, n int) memory.StoredFix {
	h.t.Helper()
	for _, p := range h.fixes() {
		if p.Fix.RemediationRound == round && p.Fix.Try == n {
			return p
		}
	}
	h.t.Fatalf("no fix row for round %d, try %d", round, n)
	return memory.StoredFix{}
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

// proposedPayload is the tenant a fix.proposed:v1 entry is stored under
// and the event payload it wraps.
func proposedPayload(t *testing.T, e outbox.Entry) (tenantID string, payload []byte) {
	t.Helper()
	tenantID, payload, err := outbox.UnwrapTenant(e.Payload)
	require.NoError(t, err)
	return tenantID, payload
}

// proposedEvents is every outbox entry on fix.proposed:v1.
func (h *harness) proposedEvents() []outbox.Entry {
	var out []outbox.Entry
	for _, e := range h.store.OutboxEntries() {
		if e.StreamName == streams.FixProposedV1 {
			out = append(out, e)
		}
	}
	return out
}
