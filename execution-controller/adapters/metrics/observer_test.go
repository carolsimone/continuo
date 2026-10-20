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
