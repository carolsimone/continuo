package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/liveness"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// readiness and liveness build the two health handlers exactly as main wires
// them (readiness → Check, liveness → LivenessCheck), so these tests exercise
// the real registry-check split plumbing.
func readiness(reg *liveness.Registry) http.HandlerFunc {
	return liveness.Handler("readiness", reg.Check, discardLogger())
}
func liveHandler(reg *liveness.Registry) http.HandlerFunc {
	return liveness.Handler("liveness", reg.LivenessCheck, discardLogger())
}

func code(h http.HandlerFunc) int {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Code
}

// TestHealthyRegistryPassesBothProbes: a live worker + fresh heartbeat → both
// readiness and liveness answer 200.
func TestHealthyRegistryPassesBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddWorkerProbe("remediation_requested_heartbeat", 0, func(context.Context) error { return nil })

	if c := code(readiness(reg)); c != http.StatusOK {
		t.Fatalf("readiness: expected 200, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusOK {
		t.Fatalf("liveness: expected 200, got %d", c)
	}
}

// TestDependencyOutageFailsReadinessNotLiveness is the [P1a] regression: a
// failing dependency probe fails readiness but NOT liveness.
func TestDependencyOutageFailsReadinessNotLiveness(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddDependencyProbe("postgres", 0, func(context.Context) error { return errors.New("down") })

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness during dependency outage: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusOK {
		t.Fatalf("liveness during dependency outage: expected 200 (must NOT restart), got %d", c)
	}
}

// TestConsumerExitFailsBothProbes: a consumer that exited with an error fails
// both readiness and liveness.
func TestConsumerExitFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.WorkerExited("remediation_requested", errors.New("permanent bootstrap failure: WRONGTYPE"))

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness after consumer exit: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("liveness after consumer exit: expected 503, got %d", c)
	}
}

// TestStaleHeartbeatFailsBothProbes: a wedged consumer (stale heartbeat) fails
// both probes.
func TestStaleHeartbeatFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddWorkerProbe("remediation_requested_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness with stale heartbeat: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("liveness with stale heartbeat: expected 503, got %d", c)
	}
}

// fakeLoop is a background loop that records that it ran.
type fakeLoop struct{ ran chan struct{} }

func newFakeLoop() *fakeLoop { return &fakeLoop{ran: make(chan struct{})} }

func (l *fakeLoop) Run(context.Context) { close(l.ran) }

func (l *fakeLoop) hasRun() bool {
	select {
	case <-l.ran:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

// TestMain_RunnerLoopsStartInLegacyMode: the worker, the gate poller and the
// sweeper run with either engine, so a remediation the runner started always
// finishes, whichever engine the install runs now.
func TestMain_RunnerLoopsStartInLegacyMode(t *testing.T) {
	for _, engine := range []string{config.EngineLegacy, config.EngineRunner} {
		t.Run(engine, func(t *testing.T) {
			worker, poller, sweeper := newFakeLoop(), newFakeLoop(), newFakeLoop()
			startRunnerLoops(context.Background(), engine, func(f func()) { go f() },
				runnerLoops{worker: worker, poller: poller, sweeper: sweeper}, discardLogger())
			assert.True(t, worker.hasRun(), "worker")
			assert.True(t, poller.hasRun(), "gate poller")
			assert.True(t, sweeper.hasRun(), "sweeper")
		})
	}
}

// TestMain_RunnerGetsTheConfiguredVerificationTimeout: both the gate's budget
// and the verifying try's deadline come from VERIFICATION_TIMEOUT. A zero in
// either would time out every verification that is running.
func TestMain_RunnerGetsTheConfiguredVerificationTimeout(t *testing.T) {
	cfg := config.Config{
		VerificationTimeout: 37 * time.Minute,
		Maintenance:         true,
		SQLDialect:          "trino",
		ServiceRepoPaths:    map[string]string{"svc": "path"},
	}
	collab := runnerCollaborators(cfg, runner.Collaborators{})
	deps := newRunnerDeps(cfg, collab, workflow.Set{}, nil, nil, "host:7", discardLogger())

	assert.Equal(t, 37*time.Minute, deps.VerificationTimeout)
	assert.Equal(t, 37*time.Minute, deps.Collab.VerificationTimeout)
	assert.Equal(t, "trino", deps.Collab.SQLDialect)
	assert.Equal(t, map[string]string{"svc": "path"}, deps.Collab.ServiceRepoPaths)
	assert.True(t, deps.Maintenance)
	assert.Equal(t, "host:7", deps.Owner)
	assert.NotNil(t, deps.Registry)
	assert.NotNil(t, deps.Collab.Clock, "the gate and the runner read the same clock")
}

func TestMain_LeaseOwnerNamesHostAndProcess(t *testing.T) {
	assert.Equal(t, "agent-remediation-5d9f:42", leaseOwner("agent-remediation-5d9f", 42))
	assert.Equal(t, "unknown:42", leaseOwner("", 42))
}

// TestMain_BuiltinFilesLoadAndValidate: the files built into the binary load
// and every flow and type of them runs against the registry.
func TestMain_BuiltinFilesLoadAndValidate(t *testing.T) {
	set, err := loadRunnerFiles(workflows.Builtin, runner.Collaborators{}, discardLogger())
	require.NoError(t, err)
	assert.Contains(t, set.Flows, "rejected-release")
}

// TestMain_RefusedFilesStopStartupAndEveryRefusalIsLogged.
func TestMain_RefusedFilesStopStartupAndEveryRefusalIsLogged(t *testing.T) {
	fsys := fstest.MapFS{
		"flows/bad.yaml": {Data: []byte(`kind: flow
version: 1
name: bad
trigger: remediation.requested
types: [nope]
across_clusters: [no_such_check]
gate: [no_such_gate]
tries: 1
output:
  on_pass: { action: pull_request, emits: remediation.proposed }
  on_give_up: { action: hand_over }
`)},
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	_, err := loadRunnerFiles(fsys, runner.Collaborators{}, logger)

	require.Error(t, err)
	refusals := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(line, `"level":"ERROR"`) {
			refusals++
		}
	}
	assert.GreaterOrEqual(t, refusals, 2, "each refusal is logged on its own line, with its file and line:\n%s", buf.String())
}

type pruneRecorder struct {
	repository.RemediationRepository
	cutoff time.Time
}

func (p *pruneRecorder) DeleteFinishedBefore(_ context.Context, cutoff time.Time) (int64, error) {
	p.cutoff = cutoff
	return 3, nil
}

type fixedNow struct{ t time.Time }

func (c fixedNow) Now() time.Time { return c.t }

func TestMain_RemediationRetentionPrunesFinishedRemediationsBeforeTheCutoff(t *testing.T) {
	now := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	repo := &pruneRecorder{}
	target := remediationRetentionTarget(repo, fixedNow{now})

	n, err := target.Prune(context.Background(), 7*24*time.Hour, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, now.Add(-7*24*time.Hour), repo.cutoff)
	assert.Equal(t, "remediation", target.Name)
	assert.Equal(t, model.ReplayHorizon, target.MinRetention, "a remediation is kept for the replay horizon at least")
}
