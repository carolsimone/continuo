package main

import (
	"bytes"
	"context"
	"errors"
	"io"
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

// TestMain_RunnerLoopsStartInLegacyMode: the loops buildRunner returns, the
// worker, the gate poller and the sweeper, each do their work with either
// engine, so a remediation the runner started finishes whichever engine runs
// now.
func TestMain_RunnerLoopsStartInLegacyMode(t *testing.T) {
	for _, engine := range []string{config.EngineLegacy, config.EngineRunner} {
		t.Run(engine, func(t *testing.T) {
			repo := &spyRepo{}
			cfg := config.Config{Engine: engine, VerificationPollInterval: time.Hour}
			loops, accept := buildRunner(cfg, runner.Collaborators{}, workflow.Set{}, runnerWiring{
				newUoW: func() uow.UnitOfWork { return nil },
				repo:   func() repository.RemediationRepository { return repo },
				owner:  "host:1",
				live:   liveness.NewRegistry(),
				logger: discardLogger(),
			})
			require.NotNil(t, accept)

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			for _, l := range []loop{loops.worker, loops.poller, loops.sweeper} {
				wg.Add(1)
				go func() { defer wg.Done(); l.Run(ctx) }()
			}
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
