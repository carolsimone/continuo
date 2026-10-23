package metrics

import (
	"testing"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRunMetrics_CountsTheStalledRuns(t *testing.T) {
	m := NewRunMetrics(pkgmetrics.New("state"))
	m.RunStalled()
	m.RunStalled()
	if got := testutil.ToFloat64(m.stalled); got != 2 {
		t.Fatalf("continuo_runs_stalled_total = %v, want 2", got)
	}
}
