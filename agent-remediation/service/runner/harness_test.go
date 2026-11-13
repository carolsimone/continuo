package runner_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// fakeSource is the repository at the trigger's commit. fail is returned by
// the next ReadFile, once, and onRead runs on every read.
type fakeSource struct {
	mu     sync.Mutex
	files  map[string]string
	fail   error
	onRead func()
}

func (s *fakeSource) ReadFile(_ context.Context, _, _, path string) (string, error) {
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

type fakeEvidence struct{ objects map[string]string }

func (e fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	c, ok := e.objects[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return c, nil
}

type nopSanitizer struct{}

func (nopSanitizer) Sanitize(s string) string { return s }

type noPrecedents struct{}

func (noPrecedents) Precedents(context.Context, ports.PrecedentQuery) ([]prompt.Precedent, error) {
	return nil, nil
}

// noBundle is a code bundle that holds no node. onRead runs on every read.
type noBundle struct {
	mu     sync.Mutex
	reads  int
	onRead func(n int)
}

func (b *noBundle) NodeSource(context.Context, string, string, string) (ports.CandidateSource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.onRead != nil {
		b.onRead(b.reads)
	}
	return ports.CandidateSource{}, ports.ErrNotFound
}

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
	return "s3://artifacts/" + key, nil
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
}

func (l *scriptedLLM) Propose(_ context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	l.mu.Lock()
	l.requests = append(l.requests, req)
	n := len(l.requests)
	answer, onCall := l.answer, l.onCall
	l.mu.Unlock()
	if onCall != nil {
		onCall(n)
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
	clock       fixedClock
	source      *fakeSource
	evidence    fakeEvidence
	bundle      *noBundle
	artifacts   *memArtifacts
	llm         *scriptedLLM
	files       workflow.Set
	maintenance bool
	runner      *runner.Runner
}

type option func(*harness)

func withMaintenance() option { return func(h *harness) { h.maintenance = true } }

func withTries(n int) option {
	return func(h *harness) { h.files = testFiles(n) }
}

func newHarness(t *testing.T, opts ...option) *harness {
	t.Helper()
	h := &harness{
		t:     t,
		store: memory.NewStore(),
		clock: fixedClock{now: t0},
		source: &fakeSource{files: map[string]string{
			"services/svc-a/models/orders.sql": "select 1",
			"services/svc-b/models/orders.sql": "select 1",
		}},
		evidence:  fakeEvidence{objects: map[string]string{}},
		bundle:    &noBundle{},
		artifacts: &memArtifacts{},
		llm:       &scriptedLLM{answer: fixAnswer},
		files:     testFiles(3),
	}
	for _, o := range opts {
		o(h)
	}
	collab := runner.Collaborators{
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
		Precedents:      noPrecedents{},
		CandidateSource: h.bundle,
		Clock:           h.clock,
	}
	reg := runner.NewRegistry(collab)
	_, err := runner.Validate(h.files, reg)
	require.NoError(t, err)
	h.runner = runner.New(runner.Deps{
		NewUoW:      func() uow.UnitOfWork { return memory.NewUnitOfWork(h.store) },
		Registry:    reg,
		Files:       h.files,
		LLM:         llmcache.New(h.llm, &memCache{}, "test-model", discard),
		Collab:      collab,
		Clock:       h.clock,
		Logger:      discard,
		Maintenance: h.maintenance,
		Owner:       workerOwner,
	})
	return h
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
	u := memory.NewUnitOfWork(h.store)
	require.NoError(h.t, u.Begin(ctx))
	require.NoError(h.t, u.ProposalRepo().Upsert(ctx, proposal.Proposal{
		Source: "compile", ReleaseID: releaseID, RemediationRound: 1, Attempt: attempt, Status: status,
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
