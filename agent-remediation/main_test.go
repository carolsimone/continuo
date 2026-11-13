package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/fixer"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
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

// spyRepo is a remediation repository that finds nothing and counts the calls
// each of the three loops makes.
type spyRepo struct {
	repository.RemediationRepository
	claims, verifying, failing atomic.Int64
}

func (s *spyRepo) ClaimNext(context.Context, string, time.Time, time.Duration) (uuid.UUID, bool, error) {
	s.claims.Add(1)
	return uuid.Nil, false, nil
}

func (s *spyRepo) ListVerifying(context.Context, int) ([]remediation.Snapshot, error) {
	s.verifying.Add(1)
	return nil, nil
}

func (s *spyRepo) ListFailingSteps(context.Context, int, int) ([]uuid.UUID, error) {
	s.failing.Add(1)
	return nil, nil
}

func (s *spyRepo) ListPastDeadline(context.Context, time.Time, int) ([]uuid.UUID, error) {
	return nil, nil
}

// TestMain_RunnerLoopsStartInLegacyMode: startRunner, which main calls with its
// lifecycle's Go, starts the worker, the gate poller and the sweeper, and each
// does its work with either engine, so a remediation the runner started
// finishes whichever engine runs now.
func TestMain_RunnerLoopsStartInLegacyMode(t *testing.T) {
	for _, engine := range []string{config.EngineLegacy, config.EngineRunner} {
		t.Run(engine, func(t *testing.T) {
			repo := &spyRepo{}
			cfg := config.Config{Engine: engine, VerificationPollInterval: time.Hour}
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			accept := startRunner(ctx, cfg, runner.Collaborators{}, workflow.Set{}, runnerWiring{
				newUoW: func() uow.UnitOfWork { return nil },
				repo:   func() repository.RemediationRepository { return repo },
				owner:  "host:1",
				live:   liveness.NewRegistry(),
				logger: discardLogger(),
			}, func(f func()) { wg.Add(1); go func() { defer wg.Done(); f() }() })
			require.NotNil(t, accept)
			defer func() { cancel(); wg.Wait() }()

			assert.Eventually(t, func() bool { return repo.claims.Load() > 0 }, 5*time.Second, time.Millisecond, "worker")
			assert.Eventually(t, func() bool { return repo.verifying.Load() > 0 }, 5*time.Second, time.Millisecond, "gate poller")
			assert.Eventually(t, func() bool { return repo.failing.Load() > 0 }, 5*time.Second, time.Millisecond, "sweeper")
		})
	}
}

// TestMain_RunnerLoopsAreRegisteredWithLiveness: the worker and the poller are
// workers of the liveness registry, as a stream consumer is. One that returns
// before shutdown fails the probes.
func TestMain_RunnerLoopsAreRegisteredWithLiveness(t *testing.T) {
	live := liveness.NewRegistry()
	_, _ = buildRunner(config.Config{Engine: config.EngineLegacy}, runner.Collaborators{}, workflow.Set{}, runnerWiring{
		newUoW: func() uow.UnitOfWork { return nil },
		repo:   func() repository.RemediationRepository { return &spyRepo{} },
		owner:  "host:1",
		live:   live,
		logger: discardLogger(),
	})
	var names []string
	for _, f := range live.LivenessCheck(context.Background()) {
		names = append(names, f.Name)
	}
	assert.Empty(t, names, "both loops start healthy")

	exited := trackedLoop{name: "remediation_worker", inner: returningLoop{}, live: live}
	exited.Run(context.Background())
	assert.NotEmpty(t, live.LivenessCheck(context.Background()), "a worker that returned while running fails liveness")
}

type returningLoop struct{}

func (returningLoop) Run(context.Context) {}

func TestMain_LoopHealthFailsWhenTheLoopStopsBeating(t *testing.T) {
	h := &loopHealth{}
	h.beat()
	assert.NoError(t, h.healthy(time.Minute))
	time.Sleep(20 * time.Millisecond)
	assert.Error(t, h.healthy(time.Millisecond))
	h.beat()
	assert.NoError(t, h.healthy(time.Minute))
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
	collab := runnerCollaborators(cfg, fixer.Services{}, nil)
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

// builtinRenamed is the built-in files with the flow every heal trigger runs
// renamed to name.
func builtinRenamed(t *testing.T, name string) fstest.MapFS {
	t.Helper()
	out := fstest.MapFS{}
	require.NoError(t, fs.WalkDir(workflows.Builtin, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(workflows.Builtin, p)
		if err != nil {
			return err
		}
		out[p] = &fstest.MapFile{Data: data}
		return nil
	}))
	flow := out["flows/rejected-release.yaml"]
	require.NotNil(t, flow)
	renamed := strings.Replace(string(flow.Data), "\nname: rejected-release\n", "\nname: "+name+"\n", 1)
	require.NotEqual(t, string(flow.Data), renamed)
	flow.Data = []byte(renamed)
	return out
}

// TestMain_FilesWithoutTheTriggerFlowStopStartup: files that validate but do
// not define the flow every heal trigger runs stop startup, with either engine,
// since every remediation the runner accepted would follow a flow no file
// defines.
func TestMain_FilesWithoutTheTriggerFlowStopStartup(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	_, err := loadRunnerFiles(builtinRenamed(t, "another-flow"), runner.Collaborators{}, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"rejected-release"`)
	assert.Contains(t, buf.String(), `"level":"ERROR"`)
	assert.Contains(t, buf.String(), "rejected-release", "the refusal is logged")
}

// TestMain_WarnsWhenTheRunnerIgnoresMaxAttempts: under the runner the flow
// file's tries govern, so a different AGENT_REMEDIATION_MAX_ATTEMPTS is
// warned about at startup. The legacy engine reads the variable, and an equal
// value changes nothing, so neither warns.
func TestMain_WarnsWhenTheRunnerIgnoresMaxAttempts(t *testing.T) {
	files, err := loadRunnerFiles(workflows.Builtin, runner.Collaborators{}, discardLogger())
	require.NoError(t, err)
	tries := files.Flows["rejected-release"].Tries

	for _, tc := range []struct {
		name        string
		engine      string
		maxAttempts int
		warns       bool
	}{
		{"runner with a different value", config.EngineRunner, tries + 2, true},
		{"runner with the flow's tries", config.EngineRunner, tries, false},
		{"legacy with a different value", config.EngineLegacy, tries + 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))

			warnIgnoredMaxAttempts(config.Config{Engine: tc.engine, MaxAttempts: tc.maxAttempts}, files, logger)

			if !tc.warns {
				assert.Empty(t, buf.String())
				return
			}
			assert.Contains(t, buf.String(), `"level":"WARN"`)
			assert.Contains(t, buf.String(), "AGENT_REMEDIATION_MAX_ATTEMPTS")
			assert.Contains(t, buf.String(), `"tries":3`)
		})
	}
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

// TestMain_RunnerCollaboratorsAreTheFixerServicesWithoutTheModel: every field
// of fixer.Services but the model is a field of runner.Collaborators of the
// same type, and a service the fixer is given reaches the runner.
func TestMain_RunnerCollaboratorsAreTheFixerServicesWithoutTheModel(t *testing.T) {
	svcType, collabType := reflect.TypeOf(fixer.Services{}), reflect.TypeOf(runner.Collaborators{})
	for i := range svcType.NumField() {
		f := svcType.Field(i)
		if f.Name == "LLM" {
			continue
		}
		c, ok := collabType.FieldByName(f.Name)
		if assert.True(t, ok, "runner.Collaborators has no field %s", f.Name) {
			assert.Equal(t, f.Type, c.Type, f.Name)
		}
	}

	logger := discardLogger()
	paths := map[string]string{"svc": "cfg-path"}
	collab := runnerCollaborators(config.Config{ServiceRepoPaths: paths}, fixer.Services{
		Logger: logger, ServiceRepoPaths: map[string]string{"svc": "other"}, SQLDialect: "postgres",
	}, nil)
	assert.Same(t, logger, collab.Logger)
	assert.Equal(t, paths, collab.ServiceRepoPaths, "the install's configuration wins")
}

// The liveness budgets cover what each loop does between two beats: a worker
// one step and a park, a poller one poll and the wait between two passes. A
// poll interval of any length keeps the probe healthy between passes.
func TestMain_LoopBudgetsCoverTheLongestSilence(t *testing.T) {
	for _, interval := range []time.Duration{15 * time.Second, 5 * time.Minute, time.Hour} {
		worker, poller := loopBudgets(config.Config{VerificationPollInterval: interval})
		assert.GreaterOrEqual(t, worker, runner.DefaultStepTimeout+runner.ParkDuration, "worker, interval %s", interval)
		assert.GreaterOrEqual(t, poller, interval+runner.DefaultStepTimeout, "poller, interval %s", interval)
	}
}

// A poller registered for a one hour poll interval is not reported wedged
// after the five minutes a fixed budget would have allowed.
func TestMain_APollerWithALongIntervalStaysHealthyBetweenPasses(t *testing.T) {
	_, budget := loopBudgets(config.Config{VerificationPollInterval: time.Hour})
	h := &loopHealth{}
	h.last.Store(time.Now().Add(-time.Hour).UnixNano())
	assert.NoError(t, h.healthy(budget))
	h.last.Store(time.Now().Add(-budget - time.Minute).UnixNano())
	assert.Error(t, h.healthy(budget))
}
