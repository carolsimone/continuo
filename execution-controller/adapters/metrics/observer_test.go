package metrics

import (
	"testing"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserver_GaugeCountsTheAttemptsAwaitingAPodStop(t *testing.T) {
	o := NewObserver(pkgmetrics.New("execution-controller"))
	o.AwaitingPodStop("a", true)
	o.AwaitingPodStop("b", true)
	o.AwaitingPodStop("a", true)
	if got := testutil.ToFloat64(o.awaiting); got != 2 {
		t.Fatalf("gauge = %v, want 2", got)
	}
	o.AwaitingPodStop("a", false)
	o.AwaitingPodStop("c", false)
	if got := testutil.ToFloat64(o.awaiting); got != 1 {
		t.Fatalf("gauge = %v, want 1", got)
	}
}

func TestObserver_CountsStoppedAttemptsAndReapedJobsByReason(t *testing.T) {
	o := NewObserver(pkgmetrics.New("execution-controller"))
	o.AttemptStopped()
	o.AttemptStopped()
	o.JobReaped("fenced_run")
	o.JobReaped("ended_undeployed")
	o.JobReaped("fenced_run")
	if got := testutil.ToFloat64(o.stopped); got != 2 {
		t.Fatalf("stopped = %v, want 2", got)
	}
	if got := testutil.ToFloat64(o.reaped.WithLabelValues("fenced_run")); got != 2 {
		t.Fatalf("fenced_run = %v, want 2", got)
	}
	if got := testutil.ToFloat64(o.reaped.WithLabelValues("ended_undeployed")); got != 1 {
		t.Fatalf("ended_undeployed = %v, want 1", got)
	}
}
