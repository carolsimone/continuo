// Package metrics holds release-controller's own Prometheus series.
package metrics

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	outcomePromoted = "promoted"
	outcomeRejected = "rejected"
	// noRejectReason is the reject_reason of a release that was not rejected.
	noRejectReason = "none"

	stageOK     = "ok"
	stageFailed = "failed"
)

// Telemetry exports the release series: how long each pipeline stage took and
// how each candidate release ended. The stage series cover every run's stage
// durations, a verification's included, and continuo_releases_total counts the
// candidates' outcomes only, so the calls that only announce a request, the
// receipt of a release and a verification's own outcome record nothing.
type Telemetry struct {
	releases      *prometheus.CounterVec
	stageDuration *prometheus.HistogramVec
}

var _ ports.Telemetry = (*Telemetry)(nil)

// NewTelemetry registers continuo_releases_total and
// continuo_release_stage_duration_seconds on reg. Every release outcome the
// pipeline can reach is exported at 0 from the start.
func NewTelemetry(reg *pkgmetrics.Registry) *Telemetry {
	t := &Telemetry{
		releases: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_releases_total",
			Help: "Candidate releases release-controller ended, by outcome (promoted or rejected) and reject_reason (none for a promoted release).",
		}, []string{"outcome", "reject_reason"}),
		stageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "continuo_release_stage_duration_seconds",
			Help:    "Seconds a run spent in a pipeline stage (compiling, parsing, seed_building or validating), by the stage's own outcome: ok or failed.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200},
		}, []string{"stage", "outcome"}),
	}
	t.releases.WithLabelValues(outcomePromoted, noRejectReason)
	for _, reason := range model.RejectReasons() {
		t.releases.WithLabelValues(outcomeRejected, string(reason))
	}
	reg.Register(t.releases, t.stageDuration)
	return t
}

func (t *Telemetry) observeStage(stage pipeline.Status, ok bool, took time.Duration) {
	outcome := stageFailed
	if ok {
		outcome = stageOK
	}
	t.stageDuration.WithLabelValues(string(stage), outcome).Observe(took.Seconds())
}

// ReleaseCompileCompleted observes the time the run spent compiling.
func (t *Telemetry) ReleaseCompileCompleted(_ context.Context, _ string, ok bool, took time.Duration) {
	t.observeStage(pipeline.StatusCompiling, ok, took)
}

// ReleaseParseCompleted observes the time the run spent parsing.
func (t *Telemetry) ReleaseParseCompleted(_ context.Context, _ string, ok bool, took time.Duration) {
	t.observeStage(pipeline.StatusParsing, ok, took)
}

// ReleaseSeedBuildCompleted observes the time the run spent building seeds.
func (t *Telemetry) ReleaseSeedBuildCompleted(_ context.Context, _ string, ok bool, took time.Duration) {
	t.observeStage(pipeline.StatusSeedBuilding, ok, took)
}

// ReleaseValidationCompleted observes the time the run spent validating. A
// release that validates no node, and so passes straight through the stage,
// has no validation to time and observes nothing.
func (t *Telemetry) ReleaseValidationCompleted(_ context.Context, _ string, ok bool, okCount, failCount int, took time.Duration) {
	if ok && okCount+failCount == 0 {
		return
	}
	t.observeStage(pipeline.StatusValidating, ok, took)
}

// ReleasePromoted counts one promoted release.
func (t *Telemetry) ReleasePromoted(context.Context, string, int) {
	t.releases.WithLabelValues(outcomePromoted, noRejectReason).Inc()
}

// ReleaseRejected counts one rejected release under its reject reason.
func (t *Telemetry) ReleaseRejected(_ context.Context, _, reason string, _ []string) {
	t.releases.WithLabelValues(outcomeRejected, reason).Inc()
}

// ReleaseReceived records nothing.
func (*Telemetry) ReleaseReceived(context.Context, string) {}

// ReleaseParseRequested records nothing.
func (*Telemetry) ReleaseParseRequested(context.Context, string) {}

// ReleaseValidationRequested records nothing.
func (*Telemetry) ReleaseValidationRequested(context.Context, string, int) {}

// ReleaseSeedBuildRequested records nothing.
func (*Telemetry) ReleaseSeedBuildRequested(context.Context, string, int) {}

// VerificationFinished records nothing.
func (*Telemetry) VerificationFinished(context.Context, string, string, int) {}
