package observer

import (
	"context"
	"errors"
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

// probeBlock frames, with the sentinels, the result block of the view probe:
// views is the JSON array of its answers. A block that is not checked says the
// warehouse was unreachable.
func probeBlock(checked bool, views string) string {
	message, reason := "", ""
	if !checked {
		message, reason = "warehouse_unreachable: connection refused", `,"reason":"warehouse_unreachable"`
	}
	body := fmt.Sprintf(`{"schema_version":1,"status":"success","message":%q,"failures":0,"unique_id":"analytics.seed_fx_rates_eur","op":"probe_views","checked":%t,"views":%s%s}`,
		message, checked, views, reason)
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

// reportFindings are the findings of a report-mode preflight: one view
// outside the allowed set, one inside it.
func reportFindings() *seedreload.Findings {
	return &seedreload.Findings{DropSemantics: "cascade", Completeness: "exact", Checked: true, Dependents: []seedreload.Dependent{
		{Schema: "analytics", Name: "fx_rates_current", Kind: "view", Owner: "analytics", Depth: 1, Allowed: true},
		{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1}}}
}

func succeededFacts(findings *seedreload.Findings) model.AttemptFacts {
	code := int32(0)
	return model.AttemptFacts{ExitCode: &code, JobSucceeded: true, SeedPreflight: findings}
}

func observerWithProber(p *fakeProber) *Observer {
	o := unitObserver(&fakeQueue{}, &fakeObserved{})
	o.deps.Prober = p
	return o
}

func TestProbeViews_ProbesTheViewsOutsideTheAllowedSetOfASucceededLoad(t *testing.T) {
	prober := &fakeProber{log: probeBlock(true, `[{"schema":"bi","name":"fx_rates_report","ok":false,"error":"VIEW_IS_STALE"}]`)}

	got := observerWithProber(prober).probeViews(context.Background(), seedDeployment(seedreload.PreflightReport), succeededFacts(reportFindings()))

	require.Len(t, prober.requests, 1)
	assert.Equal(t, "analytics-seed-1", prober.requests[0].JobName)
	assert.Equal(t, "analytics.seed_fx_rates_eur", prober.requests[0].NodeID)
	assert.Equal(t, []string{"bi.fx_rates_report"}, prober.requests[0].Views)
	assert.Equal(t, []seedreload.Probe{{Schema: "bi", Name: "fx_rates_report", Error: "VIEW_IS_STALE"}}, got)
}

func TestProbeViews_SpellsEachViewAsThePreflightReportedIt(t *testing.T) {
	prober := &fakeProber{log: probeBlock(true, `[]`)}
	findings := reportFindings()
	findings.Dependents[1].Schema, findings.Dependents[1].Name = "BI", "Fx_Rates_Report"

	observerWithProber(prober).probeViews(context.Background(), seedDeployment(seedreload.PreflightReport), succeededFacts(findings))

	assert.Equal(t, []string{"BI.Fx_Rates_Report"}, prober.requests[0].Views)
}

func TestProbeViews_DoesNotProbeWithoutAReasonTo(t *testing.T) {
	failed := succeededFacts(reportFindings())
	code := int32(1)
	failed.JobSucceeded, failed.ExitCode = false, &code
	allAllowed := reportFindings()
	allAllowed.Dependents[1].Allowed = true
	probed := reportFindings()
	probed.Probes = []seedreload.Probe{{Schema: "bi", Name: "fx_rates_report", OK: true}}

	for name, tc := range map[string]struct {
		dep   *model.Deployment
		facts model.AttemptFacts
	}{
		"the load failed":                    {seedDeployment(seedreload.PreflightReport), failed},
		"nothing is outside the allowed set": {seedDeployment(seedreload.PreflightReport), succeededFacts(allAllowed)},
		"the preflight ran in check mode":    {seedDeployment(seedreload.PreflightCheck), succeededFacts(reportFindings())},
		"the findings hold probes already":   {seedDeployment(seedreload.PreflightReport), succeededFacts(probed)},
		"the preflight printed no findings":  {seedDeployment(seedreload.PreflightReport), succeededFacts(nil)},
		"the command carries no preflight":   {model.NewDeployment(command.DeployTask{TaskID: "t"}, nil, time.Now()), succeededFacts(reportFindings())},
		"the attempt is a release leg":       {model.NewSeedBuildDeployment(command.ValidationDeployTask{NodeID: "a.b"}, nil, time.Now()), succeededFacts(reportFindings())},
	} {
		t.Run(name, func(t *testing.T) {
			prober := &fakeProber{log: probeBlock(true, `[]`)}

			got := observerWithProber(prober).probeViews(context.Background(), tc.dep, tc.facts)

			assert.Nil(t, got)
			assert.Empty(t, prober.requests)
		})
	}
}

func TestProbeViews_AProbeThatCannotRunMarksEachViewNotProbed(t *testing.T) {
	for name, tc := range map[string]struct {
		log  string
		err  error
		want string
	}{
		"the probe fails":              {err: errors.New("deadline exceeded"), want: "not probed: deadline exceeded"},
		"the warehouse is unreachable": {log: probeBlock(false, `[]`), want: "not probed: warehouse_unreachable: connection refused"},
		"the probe prints nothing":     {want: "not probed: the probe printed no result"},
	} {
		t.Run(name, func(t *testing.T) {
			prober := &fakeProber{log: tc.log, err: tc.err}

			got := observerWithProber(prober).probeViews(context.Background(), seedDeployment(seedreload.PreflightReport), succeededFacts(reportFindings()))

			assert.Equal(t, []seedreload.Probe{{Schema: "bi", Name: "fx_rates_report", OK: false, Error: tc.want}}, got)
		})
	}
}

// The probe is bounded by probeTimeout, whatever the prober does with its context.
func TestProbeViews_BoundsTheProbeByItsTimeout(t *testing.T) {
	var deadline time.Time
	prober := &fakeProber{onProbe: func(ctx context.Context) { deadline, _ = ctx.Deadline() }, log: probeBlock(true, `[]`)}
	before := time.Now()

	observerWithProber(prober).probeViews(context.Background(), seedDeployment(seedreload.PreflightReport), succeededFacts(reportFindings()))

	assert.WithinDuration(t, before.Add(probeTimeout), deadline, 2*time.Second)
}
