package observer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/validationresult"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unitObserver(q *fakeQueue, observed *fakeObserved) *Observer {
	return New(Config{StartTimeout: time.Minute, Workers: 1}, Deps{
		Observed: observed, Cache: &fakeCache{}, Queue: q, Metrics: &fakeMetrics{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func TestProcessNext_RetriesANameThatFailedAndForgetsOneThatDidNot(t *testing.T) {
	q := &fakeQueue{ready: []string{"job-a"}}
	o := unitObserver(q, &fakeObserved{err: errBoom})
	require.True(t, o.processNext(context.Background()))
	assert.Equal(t, []string{"job-a"}, q.retried, "a failure is retried with backoff")
	assert.Equal(t, []string{"job-a"}, q.done)

	q = &fakeQueue{ready: []string{"job-b"}}
	o = unitObserver(q, &fakeObserved{})
	require.True(t, o.processNext(context.Background()))
	assert.Equal(t, []string{"job-b"}, q.forgot, "a name with no deployment is processed and forgotten")
	assert.False(t, o.processNext(context.Background()), "a shut-down queue stops the worker")
}

func TestRun_QueuesEveryStartedDeploymentAndStopsWithItsContext(t *testing.T) {
	q := &fakeQueue{}
	o := unitObserver(q, &fakeObserved{names: []string{"job-a", "job-b"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.added) == 2
	}, 5*time.Second, 10*time.Millisecond, "Run resyncs as it starts")
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop with its context")
	}
}

func TestProcess_WaitsForTheCacheToSync(t *testing.T) {
	q := &fakeQueue{}
	o := unitObserver(q, &fakeObserved{err: errBoom})
	o.deps.Cache = &fakeCache{notSynced: true}
	require.NoError(t, o.Process(context.Background(), "job-a"), "nothing is read before the cache syncs")
	assert.Equal(t, time.Second, q.after["job-a"], "the name is queued again a second later")
}

// preflightBlock frames, with the sentinels, the result block the seed
// preflight prints: an error carries one failure and a message that starts
// with its reason.
func preflightBlock(status, reason, extra string) string {
	message, failures := "", 0
	if status == "error" {
		failures = 1
		if reason != "" {
			message = reason + ": bi.fx_rates_report (view, owner bi)"
		}
	}
	reasonField := ""
	if reason != "" {
		reasonField = fmt.Sprintf(`"reason":%q,`, reason)
	}
	body := fmt.Sprintf(`{"schema_version":1,"status":%q,"message":%q,"failures":%d,"unique_id":"analytics.seed_fx_rates_eur",%s"op":"seed_preflight","drop_semantics":"cascade","completeness":"exact",%s}`,
		status, message, failures, reasonField, extra)
	return validationresult.SentinelBegin + "\n" + body + "\n" + validationresult.SentinelEnd
}

func observerWithLogs(t *testing.T, logs *fakeLogs) *Observer {
	t.Helper()
	o := unitObserver(&fakeQueue{}, &fakeObserved{})
	o.deps.Logs, o.deps.Uploader = logs, &fakeUploader{}
	return o
}

// seedDeployment is a production attempt of a seed whose pod runs a preflight
// in mode.
func seedDeployment(mode seedreload.PreflightMode) *model.Deployment {
	return model.NewDeployment(command.DeployTask{
		TaskID: "task-1", RunID: "run-1", JobName: "analytics-seed-1", ServiceName: "analytics",
		SchemaName: "analytics", TableName: "seed_fx_rates_eur", NodeType: string(pkgmodel.NodeTypeDbtSeed),
		SeedPreflight: &seedreload.Preflight{Mode: mode, Relation: "analytics.seed_fx_rates_eur"},
	}, nil, time.Now())
}

func TestReadLogs_ACheckThatFoundAnUnownedViewFailsTheAttemptUnownedDependents(t *testing.T) {
	block := preflightBlock("error", "unowned_dependents", `"checked":true,"dependents":[{"schema":"bi","name":"fx_rates_report","kind":"view","owner":"bi","depth":1,"allowed":false}]`)
	o := observerWithLogs(t, &fakeLogs{full: block, containers: map[string]string{model.SeedPreflightContainer: block}})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Equal(t, pkgmodel.FailureReasonUnownedDependents, f.Reason)
	assert.Equal(t, pkgmodel.FailureClassInvalid, f.Class())
	assert.Contains(t, f.LogMessage, "bi.fx_rates_report (view, owner bi)")
	require.NotNil(t, f.SeedPreflight)
	assert.Equal(t, "bi.fx_rates_report", f.SeedPreflight.Outside()[0].Object())
}

// The preflight's own log decides the reason: the pod's main log, which holds
// nothing of it, is no part of it.
func TestReadLogs_ACheckBlockOnlyInThePreflightLogStillDecidesTheReason(t *testing.T) {
	block := preflightBlock("error", "unowned_dependents", `"checked":true,"dependents":[{"schema":"bi","name":"fx_rates_report","kind":"view","owner":"bi","depth":1,"allowed":false}]`)
	o := observerWithLogs(t, &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: block}})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Equal(t, pkgmodel.FailureReasonUnownedDependents, f.Reason)
	assert.Contains(t, f.LogMessage, "unowned_dependents: bi.fx_rates_report")
	assert.Len(t, f.SeedPreflight.Dependents, 1)
}

func TestReadLogs_AnUnreachableWarehouseInCheckModeIsInfra(t *testing.T) {
	block := preflightBlock("error", "warehouse_unreachable", `"checked":false`)
	o := observerWithLogs(t, &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: block}})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Equal(t, pkgmodel.FailureReasonWarehouseUnreachable, f.Reason)
	assert.Equal(t, pkgmodel.FailureClassInfra, f.Class())
	require.NotNil(t, f.SeedPreflight)
	assert.False(t, f.SeedPreflight.Checked)
}

func TestReadLogs_ACheckThatSucceededSetsNoReasonAndKeepsTheFindings(t *testing.T) {
	block := preflightBlock("success", "", `"checked":true,"dependents":[{"schema":"bi","name":"fx_rates_report","kind":"view","owner":"bi","depth":1,"allowed":true}]`)
	o := observerWithLogs(t, &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: block}})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Empty(t, f.Reason)
	require.NotNil(t, f.SeedPreflight)
	assert.True(t, f.SeedPreflight.Checked)
	assert.Empty(t, f.SeedPreflight.Outside())
}

func TestReadLogs_ACheckThatCouldNotListKeepsTheExitCodeMapping(t *testing.T) {
	block := preflightBlock("error", "", `"checked":false`)
	o := observerWithLogs(t, &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: block}})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Empty(t, f.Reason, "no reason is decided, so the exit code classifies the attempt")
	require.NotNil(t, f.SeedPreflight)
	exit := int32(1)
	f.ExitCode = &exit
	assert.Equal(t, pkgmodel.FailureReasonExitCode, model.Classify(f).Reason)
}

func TestReadLogs_ReportModeNeverSetsAReason(t *testing.T) {
	for name, tc := range map[string]struct {
		block   string
		checked bool
	}{
		"what a check would conclude":  {preflightBlock("success", "unowned_dependents", `"checked":true`), true},
		"an unreachable warehouse":     {preflightBlock("success", "warehouse_unreachable", `"checked":false`), false},
		"a bad input":                  {preflightBlock("error", "", `"checked":false`), false},
		"an error that names a reason": {preflightBlock("error", "unowned_dependents", `"checked":true`), true},
	} {
		t.Run(name, func(t *testing.T) {
			o := observerWithLogs(t, &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: tc.block}})

			f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightReport))

			assert.Empty(t, f.Reason)
			require.NotNil(t, f.SeedPreflight)
			assert.Equal(t, tc.checked, f.SeedPreflight.Checked)
		})
	}
}

func TestReadLogs_APreflightWithoutABlockKeepsTheExitCodeMapping(t *testing.T) {
	o := observerWithLogs(t, &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: "Traceback (most recent call last):\n  boom"}})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Empty(t, f.Reason)
	assert.Nil(t, f.SeedPreflight)
	exit := int32(1)
	f.ExitCode = &exit
	verdict := model.Classify(f)
	assert.Equal(t, pkgmodel.FailureReasonExitCode, verdict.Reason)
	assert.Equal(t, pkgmodel.FailureClassTask, verdict.Reason.Class())
}

func TestReadLogs_APreflightLogThatCannotBeReadLeavesTheAttemptToItsExitCode(t *testing.T) {
	o := observerWithLogs(t, &fakeLogs{err: errBoom})

	f := o.readLogs(context.Background(), seedDeployment(seedreload.PreflightCheck))

	assert.Empty(t, f.Reason)
	assert.Nil(t, f.SeedPreflight)
}

// An attempt whose command asks for no preflight never reads the preflight
// container, whatever block a log holds.
func TestReadLogs_AnAttemptWithoutAPreflightReadsNoPreflightLog(t *testing.T) {
	block := preflightBlock("error", "unowned_dependents", `"checked":true`)
	logs := &fakeLogs{containers: map[string]string{model.SeedPreflightContainer: block}}
	o := observerWithLogs(t, logs)
	d := model.NewDeployment(command.DeployTask{TaskID: "task-1", JobName: "job-1", ServiceName: "s", SchemaName: "a", TableName: "b"}, nil, time.Now())

	f := o.readLogs(context.Background(), d)

	assert.Empty(t, f.Reason)
	assert.Nil(t, f.SeedPreflight)
}
