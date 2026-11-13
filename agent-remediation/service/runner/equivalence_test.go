package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/redis"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/scripted"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/agent-remediation/service/verification"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
)

// The equivalence tests run each built-in type fixture through today's driver
// and verification reconciler, and through the runner, each engine on a memory
// store of its own with fakes built from the same fixture. Both engines must
// send the same model requests, submit the same verification runs over the
// same artifact bytes, and leave proposal rows with the same statuses, node
// outcomes, verification runs and edits. Reason texts are not compared.

const (
	// equivalenceFlow is the built-in flow whose types the tests run.
	equivalenceFlow = "rejected-release"
	// equivalenceMessageID is the stream message id both engines receive the
	// trigger as.
	equivalenceMessageID = "1-0"
	// equivalenceOwner is the worker the runner claims the remediation as.
	equivalenceOwner = "equivalence"
	// equivalenceTimeout is how long a verification run may take, for both
	// engines.
	equivalenceTimeout = 20 * time.Minute
	// equivalencePasses bounds how often a test drives, polls or reconciles
	// before it gives up on the attempts ending.
	equivalencePasses = 25
)

// equivalenceScenarios are the scenarios both engines run, in run order, each
// with the statuses its attempts end in.
var equivalenceScenarios = []struct {
	scenario conformance.Scenario
	attempts []proposal.Status
}{
	{conformance.FirstDraftPasses, []proposal.Status{proposal.StatusProposed}},
	{conformance.GateFailsThenPasses, []proposal.Status{proposal.StatusFailed, proposal.StatusProposed}},
}

func TestEquivalence(t *testing.T) {
	files, err := workflowfile.Load(workflows.Builtin)
	require.NoError(t, err)
	fixtures, err := workflowfile.LoadFixtures(workflows.Builtin)
	require.NoError(t, err)

	flow, ok := files.Flows[equivalenceFlow]
	require.True(t, ok, "the built-in files define flow %s", equivalenceFlow)
	require.Len(t, flow.Types, 10, "the flow lists the ten built-in types")

	for _, typeName := range flow.Types {
		f := typeFixture(t, fixtures, typeName)
		for _, sc := range equivalenceScenarios {
			t.Run(typeName+"_"+string(sc.scenario), func(t *testing.T) {
				script := equivalenceScript(t, f, sc.scenario)
				legacy := runLegacy(t, flow, f, script)
				next := runRunner(t, files, f, script)

				require.Equal(t, sc.attempts, statusesOf(legacy.attempts), "today's attempts end as the scenario plays")
				require.NotEmpty(t, legacy.requests, "today's driver sent no model request")
				assertSameModelRequests(t, legacy.requests, next.requests)
				require.NotEmpty(t, legacy.runs, "today's driver submitted no verification run")
				assertSameVerificationRuns(t, legacy.runs, next.runs)
				assertSameAttempts(t, legacy.attempts, next.attempts)
			})
		}
	}
}

// typeFixture is the one fixture that tests the type.
func typeFixture(t *testing.T, fixtures []conformance.Fixture, typeName string) conformance.Fixture {
	t.Helper()
	var found []conformance.Fixture
	for _, f := range fixtures {
		if f.Type == typeName {
			found = append(found, f)
		}
	}
	require.Len(t, found, 1, "type %s has one fixture", typeName)
	return found[0]
}

// equivalenceScript is what the fakes answer in scenario sc of fixture f, as
// the conformance suite scripts it: the passing answers, each member's own
// passing answers, and the first attempt's runs failing in
// gate_fails_then_passes.
func equivalenceScript(t *testing.T, f conformance.Fixture, sc conformance.Scenario) conformance.Script {
	t.Helper()
	require.NotEmpty(t, f.Answers.Pass, "%s has passing answers", f.File)
	script := conformance.Script{Answers: f.Answers.Pass}
	if len(f.MemberAnswers) > 0 {
		script.MemberAnswers = make(map[string][]ports.ProposeResult, len(f.MemberAnswers))
		for node, a := range f.MemberAnswers {
			script.MemberAnswers[node] = a.Pass
		}
	}
	switch sc {
	case conformance.FirstDraftPasses:
	case conformance.GateFailsThenPasses:
		script.FailedAttempts = 1
	default:
		t.Fatalf("scenario %s is not an equivalence scenario", sc)
	}
	return script
}

// observed is what one engine did: every model request it sent, every
// verification run it submitted with the artifacts written for it, and every
// proposal row it left.
type observed struct {
	requests []string
	runs     []observedRun
	attempts []observedAttempt
}

// observedRun is one submitted verification run and the bytes of every
// artifact written under its run id, by key.
type observedRun struct {
	Request   ports.VerificationRequest
	Artifacts map[string]string
}

// observedAttempt is the compared part of one proposal row.
type observedAttempt struct {
	Attempt int
	Status  proposal.Status
	Nodes   map[string]proposal.Status
	RunIDs  []string
	Edits   []observedEdit
}

// observedEdit is the compared part of one edit.
type observedEdit struct {
	Path         string
	TargetNodeID string
}

// runLegacy runs the fixture through today's driver and verification
// reconciler on a store of its own: the trigger is proposed once, then the
// reconciler passes until no attempt is verifying.
func runLegacy(t *testing.T, flow workflow.Flow, f conformance.Fixture, script conformance.Script) observed {
	t.Helper()
	store := memory.NewStore()
	w, err := scripted.NewWorld(store, f, script)
	require.NoError(t, err)
	collab := w.Collaborators()
	llm := &recordingLLM{next: w.LLM()}
	arts := &recordingArtifacts{next: collab.Artifacts}
	logger := testLogger(t, "today's driver")
	locator := repofs.NewLocator(logger)
	newUoW := func() uow.UnitOfWork { return memory.NewUnitOfWork(store) }

	deps := handlers.Deps{
		NewUoW:            newUoW,
		LLM:               llm,
		Evidence:          collab.Evidence,
		Source:            collab.Source,
		Sanitizer:         collab.Sanitizer,
		Artifacts:         arts,
		Clock:             w.Clock(),
		Logger:            logger,
		MaxAttempts:       flow.Tries,
		ServiceRepoPaths:  collab.ServiceRepoPaths,
		Locator:           collab.Locator,
		Upstream:          collab.Upstream,
		Versions:          collab.Versions,
		Precedents:        collab.Precedents,
		CandidateSource:   collab.CandidateSource,
		RepoArchive:       collab.Archive,
		ContractLocator:   locator,
		ContractInspector: locator,
		Packager:          collab.Packager,
		Pipeline:          collab.Pipeline,
		Releases:          collab.Releases,
		PriorAttempts:     store.ProposalRepository(),
		SQLDialect:        collab.SQLDialect,
	}
	reconciler := verification.New(verification.Deps{
		Lister:   store.ProposalRepository(),
		Pipeline: collab.Pipeline,
		NewUoW:   newUoW,
		Decode:   redis.TriggerFromPayload,
		Propose: func(ctx context.Context, t handlers.Trigger) error {
			return handlers.ProposeFix(ctx, deps, t)
		},
		Clock:   w.Clock(),
		Logger:  logger,
		Timeout: equivalenceTimeout,
	})

	trigger, err := redis.TriggerFromPayload(encodeTrigger(t, f.Trigger))
	require.NoError(t, err)
	trigger.MessageID = equivalenceMessageID
	require.NoError(t, handlers.ProposeFix(ctx, deps, trigger))

	for range equivalencePasses {
		if !anyVerifying(store) {
			break
		}
		reconciler.ReconcileOnce(ctx)
	}
	require.False(t, anyVerifying(store), "today's attempts still verify after %d reconciler passes", equivalencePasses)

	return observed{
		requests: llm.marshalled(t),
		runs:     observeRuns(w.Runs(), arts),
		attempts: observeAttempts(store),
	}
}

// runRunner runs the fixture through the runner on a store of its own, as the
// service wires it: the trigger is accepted, then each pass claims the
// remediation, drives it, or polls its gate while its try is verifying, and
// releases it, until the remediation ends.
func runRunner(t *testing.T, files workflow.Set, f conformance.Fixture, script conformance.Script) observed {
	t.Helper()
	store := memory.NewStore()
	w, err := scripted.NewWorld(store, f, script)
	require.NoError(t, err)
	collab := w.Collaborators()
	arts := &recordingArtifacts{next: collab.Artifacts}
	collab.Artifacts = arts
	logger := testLogger(t, "the runner")
	locator := repofs.NewLocator(logger)
	collab.ContractLocator = locator
	collab.ContractInspector = locator
	collab.VerificationTimeout = equivalenceTimeout
	llm := &recordingLLM{next: w.LLM()}

	r := runner.New(runner.Deps{
		NewUoW:              w.NewUoW,
		Registry:            runner.NewRegistry(collab),
		Files:               files,
		LLM:                 llm,
		Collab:              collab,
		Clock:               w.Clock(),
		Logger:              logger,
		Owner:               equivalenceOwner,
		VerificationTimeout: equivalenceTimeout,
	})
	require.NoError(t, r.Accept(ctx, runner.Accepted{
		Trigger:   f.Trigger,
		Payload:   encodeTrigger(t, f.Trigger),
		MessageID: equivalenceMessageID,
		Fields:    map[string]string{},
	}))
	rems := w.Remediations()
	require.Len(t, rems, 1, "the trigger made one remediation")
	id := rems[0].ID

	repo := w.Repository()
	ended := false
	for range equivalencePasses {
		snap, err := repo.Load(ctx, id)
		require.NoError(t, err)
		if snap.Remediation.Status != remediation.StatusRunning {
			ended = true
			break
		}
		claimed, err := repo.Claim(ctx, id, equivalenceOwner, w.Clock().Now(), lease)
		require.NoError(t, err)
		require.True(t, claimed, "the remediation could be claimed")
		if snap.Try != nil && snap.Try.Status == remediation.TryVerifying {
			err = r.PollGate(ctx, snap)
		} else {
			err = r.Drive(ctx, id)
		}
		require.NoError(t, err)
		require.NoError(t, repo.ReleaseLease(ctx, id, equivalenceOwner))
	}
	require.True(t, ended, "the remediation did not end after %d passes", equivalencePasses)

	return observed{
		requests: llm.marshalled(t),
		runs:     observeRuns(w.Runs(), arts),
		attempts: observeAttempts(store),
	}
}

// encodeTrigger is the remediation.requested:v2 payload of the trigger, the
// bytes both engines receive.
func encodeTrigger(t *testing.T, trig remediation.Trigger) []byte {
	t.Helper()
	payload, err := serialization.EncodeRemediationRequestedV2(trig)
	require.NoError(t, err)
	return payload
}

// anyVerifying reports whether a proposal row is still verifying.
func anyVerifying(store *memory.Store) bool {
	for _, p := range store.Proposals() {
		if p.Proposal.Status == proposal.StatusVerifying {
			return true
		}
	}
	return false
}

// observeRuns pairs each submitted run, in submit order, with the artifacts
// written under its run id.
func observeRuns(runs []ports.VerificationRequest, arts *recordingArtifacts) []observedRun {
	out := make([]observedRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, observedRun{Request: r, Artifacts: arts.under(r.Service + "/" + r.RunID + "/")})
	}
	return out
}

// observeAttempts is the compared part of every proposal row, in attempt
// order.
func observeAttempts(store *memory.Store) []observedAttempt {
	rows := store.Proposals()
	slices.SortStableFunc(rows, func(a, b memory.StoredProposal) int { return a.Proposal.Attempt - b.Proposal.Attempt })
	out := make([]observedAttempt, 0, len(rows))
	for _, row := range rows {
		p := row.Proposal
		a := observedAttempt{Attempt: p.Attempt, Status: p.Status, Nodes: map[string]proposal.Status{}}
		for node, o := range p.NodeOutcomes {
			a.Nodes[node] = o.Status
		}
		for _, v := range p.Verifications {
			a.RunIDs = append(a.RunIDs, v.RunID)
		}
		for _, e := range p.Edits {
			a.Edits = append(a.Edits, observedEdit{Path: e.Path, TargetNodeID: e.TargetNodeID})
		}
		out = append(out, a)
	}
	return out
}

// statusesOf is the status of every attempt, in attempt order.
func statusesOf(attempts []observedAttempt) []proposal.Status {
	out := make([]proposal.Status, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, a.Status)
	}
	return out
}

// assertSameModelRequests compares the model requests, in call order, as the
// JSON of each request.
func assertSameModelRequests(t *testing.T, legacy, next []string) {
	t.Helper()
	assert.Len(t, next, len(legacy), "the runner sent %d model requests, today's driver %d", len(next), len(legacy))
	for i := range min(len(legacy), len(next)) {
		assert.Equal(t, legacy[i], next[i], "model request %d", i+1)
	}
}

// assertSameVerificationRuns compares the verification runs, in submit order,
// and the bytes of the artifacts written for each.
func assertSameVerificationRuns(t *testing.T, legacy, next []observedRun) {
	t.Helper()
	assert.Len(t, next, len(legacy), "the runner submitted %d verification runs, today's driver %d", len(next), len(legacy))
	for i := range min(len(legacy), len(next)) {
		assert.Equal(t, legacy[i].Request, next[i].Request, "verification request %d", i+1)
		require.NotEmpty(t, legacy[i].Artifacts, "today's driver wrote no artifact for run %s", legacy[i].Request.RunID)
		assert.Equal(t, slices.Sorted(maps.Keys(legacy[i].Artifacts)), slices.Sorted(maps.Keys(next[i].Artifacts)),
			"the artifacts written for verification run %d", i+1)
		for key, body := range legacy[i].Artifacts {
			assert.True(t, body == next[i].Artifacts[key], "the bytes of artifact %s differ", key)
		}
		if uri := legacy[i].Request.SourceOverlayURI; uri != "" {
			assert.True(t, slices.ContainsFunc(slices.Collect(maps.Keys(legacy[i].Artifacts)), func(key string) bool {
				return strings.HasSuffix(uri, "/"+key)
			}), "run %s's overlay %s is one of its artifacts", legacy[i].Request.RunID, uri)
		}
	}
}

// assertSameAttempts compares the proposal rows, in attempt order.
func assertSameAttempts(t *testing.T, legacy, next []observedAttempt) {
	t.Helper()
	assert.Equal(t, legacy, next, "the proposal rows: attempt, status, node outcomes, verification runs and edits")
}

// recordingLLM records every request an engine sends, before the response
// cache, and passes it on.
type recordingLLM struct {
	mu   sync.Mutex
	next ports.LLMProvider
	reqs []prompt.ProposeRequest
}

func (l *recordingLLM) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	l.mu.Lock()
	l.reqs = append(l.reqs, req)
	l.mu.Unlock()
	return l.next.Propose(ctx, req)
}

// marshalled is every recorded request as JSON, in call order.
func (l *recordingLLM) marshalled(t *testing.T) []string {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.reqs))
	for _, r := range l.reqs {
		b, err := json.Marshal(r)
		require.NoError(t, err)
		out = append(out, string(b))
	}
	return out
}

// recordingArtifacts records the last body written under every key and passes
// each write on.
type recordingArtifacts struct {
	mu      sync.Mutex
	next    ports.ArtifactWriter
	written map[string]string
}

func (a *recordingArtifacts) Write(ctx context.Context, key, body, contentType string) (string, error) {
	a.mu.Lock()
	if a.written == nil {
		a.written = map[string]string{}
	}
	a.written[key] = body
	a.mu.Unlock()
	return a.next.Write(ctx, key, body, contentType)
}

// under is every recorded artifact whose key starts with prefix.
func (a *recordingArtifacts) under(prefix string) map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[string]string{}
	for key, body := range a.written {
		if strings.HasPrefix(key, prefix) {
			out[key] = body
		}
	}
	return out
}

// testLogger is a logger that keeps every warning and error, and prints them
// when the test fails, labelled with the engine that logged them.
func testLogger(t *testing.T, engine string) *slog.Logger {
	t.Helper()
	var mu sync.Mutex
	var buf bytes.Buffer
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		if t.Failed() && buf.Len() > 0 {
			t.Logf("%s logged:\n%s", engine, buf.String())
		}
	})
	return slog.New(slog.NewTextHandler(lockedWriter{mu: &mu, w: &buf}, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// lockedWriter serialises writes to w.
type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
