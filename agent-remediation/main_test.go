package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	rametrics "github.com/carolsimone/continuo/agent-remediation/adapters/metrics"
	"github.com/carolsimone/continuo/agent-remediation/config"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
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
	reg.RegisterWorker("heal_triggered")
	reg.AddWorkerProbe("heal_triggered_heartbeat", 0, func(context.Context) error { return nil })

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
	reg.RegisterWorker("heal_triggered")
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
	reg.RegisterWorker("heal_triggered")
	reg.WorkerExited("heal_triggered", errors.New("permanent bootstrap failure: WRONGTYPE"))

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
	reg.RegisterWorker("heal_triggered")
	reg.AddWorkerProbe("heal_triggered_heartbeat", 0, func(context.Context) error {
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

// inFlightRepo is a proposal repository that records the reason
// FailLegacyInFlight is called with and answers with moved and err.
type inFlightRepo struct {
	repository.ProposalRepository
	reasons []string
	moved   int
	err     error
}

func (r *inFlightRepo) FailLegacyInFlight(_ context.Context, reason string) (int, error) {
	r.reasons = append(r.reasons, reason)
	return r.moved, r.err
}

// TestMain_FailLegacyInFlight: the startup step asks the repository to fail
// the attempt rows in flight that belong to no remediation with the reason the
// release page shows, logs how many it moved (at Warn when it moved any, at
// Info when it moved none), and returns the repository's error.
func TestMain_FailLegacyInFlight(t *testing.T) {
	const reason = "continuo changed its remediation engine while this attempt was in flight; use Try again"
	for _, tc := range []struct {
		name      string
		repo      *inFlightRepo
		wantLevel string
		wantCount float64
		wantErr   bool
	}{
		{name: "none moved", repo: &inFlightRepo{}, wantLevel: "INFO"},
		{name: "some moved", repo: &inFlightRepo{moved: 3}, wantLevel: "WARN", wantCount: 3},
		{name: "the repository fails", repo: &inFlightRepo{err: errors.New("database is down")}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))

			err := failLegacyInFlight(context.Background(), tc.repo, logger)

			require.Equal(t, []string{reason}, tc.repo.reasons)
			if tc.wantErr {
				require.ErrorIs(t, err, tc.repo.err)
				assert.Empty(t, logLines(t, &buf), "an error is the caller's to log")
				return
			}
			require.NoError(t, err)
			lines := logLines(t, &buf)
			require.Len(t, lines, 1, buf.String())
			assert.Equal(t, tc.wantLevel, lines[0]["level"])
			assert.Equal(t, tc.wantCount, lines[0]["count"])
		})
	}
}

// TestMain_SweepAtStartup: a sweep that succeeds lets startup go on, and a
// failed one stops it. A failure while the context is done is a shutdown that
// cut the sweep short, whatever error the driver reported for it, so it stops
// cleanly (exit 0, logged at Info) as the other startup waits do; any other
// failure stops with exit 1, logged at Error.
func TestMain_SweepAtStartup(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		repo      *inFlightRepo
		wantStop  bool
		wantCode  int
		wantLevel string
	}{
		{name: "the sweep succeeds", ctx: context.Background(), repo: &inFlightRepo{moved: 2}, wantLevel: "WARN"},
		{name: "the sweep fails", ctx: context.Background(), repo: &inFlightRepo{err: errors.New("database is down")}, wantStop: true, wantCode: 1, wantLevel: "ERROR"},
		{name: "a shutdown cuts the sweep short with the driver's own error", ctx: cancelled, repo: &inFlightRepo{err: errors.New("driver: bad connection")}, wantStop: true, wantCode: 0, wantLevel: "INFO"},
		{name: "a shutdown cuts the sweep short with context.Canceled", ctx: cancelled, repo: &inFlightRepo{err: context.Canceled}, wantStop: true, wantCode: 0, wantLevel: "INFO"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))

			stop, code := sweepAtStartup(tc.ctx, tc.repo, logger)

			assert.Equal(t, tc.wantStop, stop)
			assert.Equal(t, tc.wantCode, code)
			lines := logLines(t, &buf)
			require.Len(t, lines, 1, buf.String())
			assert.Equal(t, tc.wantLevel, lines[0]["level"])
		})
	}
}

// TestMain_RunnerLoopsStart verifies that startRunner, which main calls with
// its lifecycle's Go, starts the worker, the gate poller and the sweeper, and
// that each does its work.
func TestMain_RunnerLoopsStart(t *testing.T) {
	repo := &spyRepo{}
	cfg := config.Config{ReleaseCheckPollInterval: time.Hour}
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
}

// TestMain_RunnerLoopsAreRegisteredWithLiveness: the worker and the poller are
// workers of the liveness registry, as a stream consumer is. One that returns
// before shutdown fails the probes.
func TestMain_RunnerLoopsAreRegisteredWithLiveness(t *testing.T) {
	live := liveness.NewRegistry()
	_, _ = buildRunner(config.Config{}, runner.Collaborators{}, workflow.Set{}, runnerWiring{
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

// TestMain_RunnerGetsTheConfiguredReleaseCheckTimeout: both the gate's budget
// and the verifying try's deadline come from RELEASE_CHECK_TIMEOUT. A zero in
// either would time out every release check that is running.
func TestMain_RunnerGetsTheConfiguredReleaseCheckTimeout(t *testing.T) {
	cfg := config.Config{
		ReleaseCheckTimeout: 37 * time.Minute,
		Maintenance:         true,
		SQLDialect:          "trino",
		ProjectRepoPaths:    map[string]string{"svc": "path"},
	}
	collab := runnerCollaborators(cfg, runnerAdapters{}, discardLogger())
	counted := rametrics.NewRemediationMetrics(pkgmetrics.New(config.ComponentName))
	deps := newRunnerDeps(cfg, collab, workflow.Set{}, runnerWiring{owner: "host:7", logger: discardLogger(), metrics: counted})

	assert.Equal(t, 37*time.Minute, deps.ReleaseCheckTimeout)
	assert.Equal(t, 37*time.Minute, deps.Collab.ReleaseCheckTimeout)
	assert.Equal(t, "trino", deps.Collab.SQLDialect)
	assert.Equal(t, map[string]string{"svc": "path"}, deps.Collab.ProjectRepoPaths)
	assert.True(t, deps.Maintenance)
	assert.Equal(t, "host:7", deps.Owner)
	assert.NotNil(t, deps.Registry)
	assert.NotNil(t, deps.Collab.Clock, "the gate and the runner read the same clock")
	assert.Same(t, counted, deps.Metrics, "the runner counts on the metrics main built")
}

func TestMain_LeaseOwnerNamesHostAndProcess(t *testing.T) {
	assert.Equal(t, "agent-remediation-5d9f:42", leaseOwner("agent-remediation-5d9f", 42))
	assert.Equal(t, "unknown:42", leaseOwner("", 42))
}

// TestMain_BuiltinFilesLoadAndValidate: the files built into the binary load
// and every flow and type of them runs against the registry.
func TestMain_BuiltinFilesLoadAndValidate(t *testing.T) {
	set, err := loadRunnerFiles(workflows.Builtin, nil, runner.Collaborators{}, discardLogger())
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
// not define the flow every heal trigger runs stop startup, since every
// remediation the runner accepted would follow a flow no file defines.
func TestMain_FilesWithoutTheTriggerFlowStopStartup(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	_, err := loadRunnerFiles(builtinRenamed(t, "another-flow"), nil, runner.Collaborators{}, logger)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"rejected-release"`)
	assert.Contains(t, buf.String(), `"level":"ERROR"`)
	assert.Contains(t, buf.String(), "rejected-release", "the refusal is logged")
}

// builtinRenamedInstall is an install folder holding the built-in flow
// rejected-release renamed to name.
func builtinRenamedInstall(t *testing.T, name string) fstest.MapFS {
	t.Helper()
	flow := builtinFile(t, "flows/rejected-release.yaml")
	renamed := strings.Replace(flow, "\nname: rejected-release\n", "\nname: "+name+"\n", 1)
	require.NotEqual(t, flow, renamed)
	return fstest.MapFS{name + ".yaml": {Data: []byte(renamed)}}
}

// TestMain_AFlowNoHealTriggerRunsIsLoggedAtWarn: every heal trigger runs
// rejected-release, so startup warns about a flow of another name, which loads
// and validates but never runs.
func TestMain_AFlowNoHealTriggerRunsIsLoggedAtWarn(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	set, err := loadRunnerFiles(workflows.Builtin, builtinRenamedInstall(t, "my-flow"), runner.Collaborators{}, logger)

	require.NoError(t, err)
	assert.Contains(t, set.Flows, "my-flow")
	var noted bool
	for _, e := range logLines(t, &buf) {
		if e["warning"] == "flow my-flow: no heal trigger runs it" {
			noted = true
			assert.Equal(t, "WARN", e["level"])
		}
	}
	assert.True(t, noted, "the flow is noted at Warn:\n%s", buf.String())
}

// TestMain_RefusedFilesStopStartupAndEveryRefusalIsLogged.
func TestMain_RefusedFilesStopStartupAndEveryRefusalIsLogged(t *testing.T) {
	fsys := fstest.MapFS{
		"flows/bad.yaml": {Data: []byte(`kind: flow
version: 1
name: bad
trigger: heal.triggered
types: [nope]
across_clusters: [no_such_check]
gate: [no_such_gate]
tries: 1
model_may_see: [code, logs]
output:
  on_pass: { action: pull_request, emits: remediation.proposed }
  on_give_up: { action: hand_over }
approver: node_owner
`)},
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	_, err := loadRunnerFiles(fsys, nil, runner.Collaborators{}, logger)

	require.Error(t, err)
	refusals := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(line, `"level":"ERROR"`) {
			refusals++
		}
	}
	assert.GreaterOrEqual(t, refusals, 2, "each refusal is logged on its own line, with its file and line:\n%s", buf.String())
}

// TestMain_RunnerCollaboratorsFillEveryField: from a fully configured install
// and every adapter main builds, every field of runner.Collaborators is set, so
// no step is built over a missing port, and the install's configuration
// reaches the steps.
func TestMain_RunnerCollaboratorsFillEveryField(t *testing.T) {
	cfg := config.Config{
		ProjectRepoPaths:    map[string]string{"svc": "cfg-path"},
		SQLDialect:          "trino",
		ReleaseCheckTimeout: 37 * time.Minute,
	}
	logger := discardLogger()
	// Each adapter is a value of the ports it answers, standing in for the
	// adapter main builds; runnerCollaborators never calls one.
	collab := runnerCollaborators(cfg, runnerAdapters{
		github: struct {
			ports.SourceReader
			ports.RepoArchive
		}{},
		evidence:  struct{ ports.EvidenceReader }{},
		artifacts: struct{ ports.ArtifactWriter }{},
		graph: struct {
			ports.NodeLocator
			ports.UpstreamChangeReader
			ports.VersionReader
			ports.PrecedentReader
			ports.CurrentContractReader
		}{},
		bundle: struct{ ports.CandidateSourceReader }{},
		contracts: struct {
			ports.ContractLocator
			ports.ContractInspector
		}{},
		packager: struct{ ports.ContractPackager }{},
		images:   struct{ ports.ImagePackagesSource }{},
		release: struct {
			ports.ReleaseReader
			ports.ReleaseChecks
		}{},
		proposals: struct{ repository.AttemptLister }{},
		tries:     struct{ repository.TryLister }{},
	}, logger)

	v := reflect.ValueOf(collab)
	for i := range v.NumField() {
		assert.False(t, v.Field(i).IsZero(), "runner.Collaborators.%s is not set", v.Type().Field(i).Name)
	}
	assert.Same(t, logger, collab.Logger)
	assert.Equal(t, cfg.ProjectRepoPaths, collab.ProjectRepoPaths)
	assert.Equal(t, "trino", collab.SQLDialect)
	assert.Equal(t, 37*time.Minute, collab.ReleaseCheckTimeout)
}

// The liveness budgets cover what each loop does between two beats: a worker
// one step and a park, a poller one poll and the wait between two passes. A
// poll interval of any length keeps the probe healthy between passes.
func TestMain_LoopBudgetsCoverTheLongestSilence(t *testing.T) {
	for _, interval := range []time.Duration{15 * time.Second, 5 * time.Minute, time.Hour} {
		worker, poller := loopBudgets(config.Config{ReleaseCheckPollInterval: interval})
		assert.GreaterOrEqual(t, worker, runner.DefaultStepTimeout+runner.ParkDuration, "worker, interval %s", interval)
		assert.GreaterOrEqual(t, poller, interval+runner.DefaultStepTimeout, "poller, interval %s", interval)
	}
}

// A poller registered for a one hour poll interval is not reported wedged
// after the five minutes a fixed budget would have allowed.
func TestMain_APollerWithALongIntervalStaysHealthyBetweenPasses(t *testing.T) {
	_, budget := loopBudgets(config.Config{ReleaseCheckPollInterval: time.Hour})
	h := &loopHealth{}
	h.last.Store(time.Now().Add(-time.Hour).UnixNano())
	assert.NoError(t, h.healthy(budget))
	h.last.Store(time.Now().Add(-budget - time.Minute).UnixNano())
	assert.Error(t, h.healthy(budget))
}

// logLines parses the JSON log lines buf holds.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), line)
		out = append(out, entry)
	}
	return out
}

// builtinFile is the content of a built-in file.
func builtinFile(t *testing.T, name string) string {
	t.Helper()
	data, err := fs.ReadFile(workflows.Builtin, name)
	require.NoError(t, err)
	return string(data)
}

// TestMain_InstallFilesFolder: a folder that does not exist (or is not named)
// means the install has no files, a folder is read as a file system, and a
// path that exists but is not a readable folder is an error, so startup stops
// rather than running without the files the install put there.
func TestMain_InstallFilesFolder(t *testing.T) {
	t.Run("a missing folder is no install files", func(t *testing.T) {
		fsys, err := installFiles(filepath.Join(t.TempDir(), "absent"))

		require.NoError(t, err)
		assert.Nil(t, fsys)
	})

	t.Run("no folder named is no install files", func(t *testing.T) {
		fsys, err := installFiles("")

		require.NoError(t, err)
		assert.Nil(t, fsys)
	})

	t.Run("a file is refused", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "workflows")
		require.NoError(t, os.WriteFile(file, []byte("kind: type\n"), 0o600))

		fsys, err := installFiles(file)

		require.Error(t, err)
		assert.Nil(t, fsys)
		assert.Contains(t, err.Error(), file)
		assert.Contains(t, err.Error(), "not a directory")
	})

	t.Run("a folder with a file in it", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "t.yaml"), []byte("kind: type\n"), 0o600))

		fsys, err := installFiles(dir)

		require.NoError(t, err)
		data, err := fs.ReadFile(fsys, "t.yaml")
		require.NoError(t, err)
		assert.Equal(t, "kind: type\n", string(data))
	})

	t.Run("an empty folder", func(t *testing.T) {
		fsys, err := installFiles(t.TempDir())

		require.NoError(t, err)
		assert.NotNil(t, fsys)
	})

	// The listing is replaced by one that fails, so the refusal does not depend
	// on the permissions of the user running the test (root lists every folder).
	t.Run("a folder that cannot be listed is refused", func(t *testing.T) {
		dir := t.TempDir()
		var listed []string
		failing := func(name string) ([]fs.DirEntry, error) {
			listed = append(listed, name)
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
		}

		fsys, err := installFilesListing(dir, failing)

		require.Error(t, err)
		assert.Nil(t, fsys)
		assert.Equal(t, []string{dir}, listed)
		assert.ErrorIs(t, err, fs.ErrPermission)
		assert.Contains(t, err.Error(), dir)
	})

	t.Run("a folder that can be listed is read through the listing it was given", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "t.yaml"), []byte("kind: type\n"), 0o600))
		var listed []string
		listing := func(name string) ([]fs.DirEntry, error) {
			listed = append(listed, name)
			return os.ReadDir(name)
		}

		fsys, err := installFilesListing(dir, listing)

		require.NoError(t, err)
		require.NotNil(t, fsys)
		assert.Equal(t, []string{dir}, listed)
	})
}

// TestMain_InstallFilesCanBeAKubernetesConfigMapMount: the chart mounts the
// install's files as a ConfigMap, whose volume holds each file as a symlink
// through ..data into a timestamped directory. Each file loads once.
func TestMain_InstallFilesCanBeAKubernetesConfigMapMount(t *testing.T) {
	dir := t.TempDir()
	stamp := "..2026_11_19_10_00_00.0123456789"
	require.NoError(t, os.Mkdir(filepath.Join(dir, stamp), 0o750))
	tuned := builtinFile(t, "types/dbt-compile.yaml") + "# tuned for this install\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, stamp, "dbt-compile.yaml"), []byte(tuned), 0o600))
	require.NoError(t, os.Symlink(stamp, filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink(filepath.Join("..data", "dbt-compile.yaml"), filepath.Join(dir, "dbt-compile.yaml")))
	install, err := installFiles(dir)
	require.NoError(t, err)

	set, err := loadRunnerFiles(workflows.Builtin, install, runner.Collaborators{}, discardLogger())

	require.NoError(t, err)
	assert.Equal(t, "dbt-compile.yaml", set.Types["dbt-compile"].File)
}

// TestMain_InstallTypeListedByAReplacedFlow: an install replaces the flow every
// heal trigger runs with a copy that lists one more type, and adds that type.
// Both load, and the flow lists the install's type after the built-in ones.
func TestMain_InstallTypeListedByAReplacedFlow(t *testing.T) {
	builtin, err := loadRunnerFiles(workflows.Builtin, nil, runner.Collaborators{}, discardLogger())
	require.NoError(t, err)
	builtinTypes := builtin.Flows["rejected-release"].Types
	lastBuiltin := builtinTypes[len(builtinTypes)-1]
	flow := strings.Replace(builtinFile(t, "flows/rejected-release.yaml"),
		"  - "+lastBuiltin+"\n", "  - "+lastBuiltin+"\n  - dbt-test-validation\n", 1)
	require.Contains(t, flow, "dbt-test-validation")
	typ := strings.NewReplacer(
		"name: dbt-validation", "name: dbt-test-validation",
		"node_types: [dbt-model, dbt-seed, dbt-snapshot, none]", "node_types: [dbt-test]",
	).Replace(builtinFile(t, "types/dbt-validation.yaml"))
	install := fstest.MapFS{
		"rejected-release.yaml":    {Data: []byte(flow)},
		"dbt-test-validation.yaml": {Data: []byte(typ)},
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	set, err := loadRunnerFiles(workflows.Builtin, install, runner.Collaborators{}, logger)

	require.NoError(t, err)
	assert.Contains(t, set.Types, "dbt-test-validation")
	assert.Equal(t, append(append([]string{}, builtinTypes...), "dbt-test-validation"),
		set.Flows["rejected-release"].Types, "the install's type follows every built-in type")
	assert.Equal(t, "rejected-release.yaml", set.Flows["rejected-release"].File)
	var replaced bool
	for _, e := range logLines(t, &buf) {
		if e["warning"] == "flow rejected-release: replaced by rejected-release.yaml" {
			replaced = true
			assert.Equal(t, "WARN", e["level"])
		}
	}
	assert.True(t, replaced, "the replacement is logged at Warn:\n%s", buf.String())
}

// TestMain_AnInstallsReplacementIsLoggedBeforeTheFilesAreChecked: every note of
// the install load is logged before the steps of the files are checked, so a
// file refused by that check still shows which built-in it was meant to
// replace.
func TestMain_AnInstallsReplacementIsLoggedBeforeTheFilesAreChecked(t *testing.T) {
	typ := strings.Replace(builtinFile(t, "types/dbt-compile.yaml"), "[target_file_was_shown,", "[no_such_check, target_file_was_shown,", 1)
	require.Contains(t, typ, "no_such_check")
	install := fstest.MapFS{"dbt-compile.yaml": {Data: []byte(typ)}}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	_, err := loadRunnerFiles(workflows.Builtin, install, runner.Collaborators{}, logger)

	require.Error(t, err)
	noteAt, refusalAt := -1, -1
	for i, e := range logLines(t, &buf) {
		switch {
		case e["warning"] == "type dbt-compile: replaced by dbt-compile.yaml":
			noteAt = i
		case e["level"] == "ERROR" && strings.Contains(e["refusal"].(string), "no_such_check"):
			refusalAt = i
		}
	}
	require.GreaterOrEqual(t, noteAt, 0, buf.String())
	require.GreaterOrEqual(t, refusalAt, 0, buf.String())
	assert.Less(t, noteAt, refusalAt)
}

// TestMain_ARefusedInstallFileStopsStartup: an install file the loader refuses
// stops startup, and each refusal is logged with the file and line.
func TestMain_ARefusedInstallFileStopsStartup(t *testing.T) {
	install := fstest.MapFS{"typo.yaml": {Data: []byte("kind: type\nversion: 1\nname: typo\nmatch: { cluster: single, source: valdation }\n")}}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	set, err := loadRunnerFiles(workflows.Builtin, install, runner.Collaborators{}, logger)

	require.Error(t, err)
	assert.Empty(t, set.Flows)
	assert.Contains(t, err.Error(), "typo.yaml:4:")
	var logged bool
	for _, e := range logLines(t, &buf) {
		if e["level"] == "ERROR" && strings.Contains(e["refusal"].(string), "typo.yaml:4:") {
			logged = true
		}
	}
	assert.True(t, logged, buf.String())
}

// TestMain_NoInstallFilesLoadsTheBuiltIns: with no install, and with an install
// folder holding nothing, the built-in files load alone and nothing is logged.
func TestMain_NoInstallFilesLoadsTheBuiltIns(t *testing.T) {
	want, err := loadRunnerFiles(workflows.Builtin, nil, runner.Collaborators{}, discardLogger())
	require.NoError(t, err)

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	got, err := loadRunnerFiles(workflows.Builtin, fstest.MapFS{}, runner.Collaborators{}, logger)

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Empty(t, buf.String())
}
