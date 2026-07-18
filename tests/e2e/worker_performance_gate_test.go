//go:build e2e_worker

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEvaluatePerfGate pins the skew-robust behavior of the performance gate
// without the stack. The Job baseline is a cross-clock measurement, so on this
// dev host it swings from a plausible positive value to a negative one across
// runs (reservation on the Postgres clock, container start on the kubelet
// clock). The gate must enforce the worker/Job ratio when that baseline is
// clock-consistent and fall back to the single-clock absolute bound when it is
// not — never red for a reason that is the host's clock rather than the product.
func TestEvaluatePerfGate(t *testing.T) {
	t.Run("plausible job baseline enforces the ratio and passes a fast worker", func(t *testing.T) {
		// worker 3ms, job 370ms (the observed positive-skew draw): ratio applies,
		// 3ms <= 0.20*370ms = 74ms.
		d := evaluatePerfGate(3*time.Millisecond, 370*time.Millisecond)
		require.True(t, d.AbsoluteOK)
		require.True(t, d.RatioApplicable)
		require.True(t, d.RatioOK)
	})

	t.Run("plausible job baseline fails a worker that exceeds a fifth", func(t *testing.T) {
		// worker 100ms, job 370ms: ratio applies, 100ms > 74ms — the ratio gate
		// still has teeth when the baseline is trustworthy.
		d := evaluatePerfGate(100*time.Millisecond, 370*time.Millisecond)
		require.True(t, d.AbsoluteOK)
		require.True(t, d.RatioApplicable)
		require.False(t, d.RatioOK)
	})

	t.Run("negative job baseline skips the ratio but keeps the absolute bound", func(t *testing.T) {
		// job p95 -43ms (the observed negative-skew draw): host clock skew, not a
		// real Job time. Ratio is not asserted; the worker's 3ms still passes the
		// absolute bound.
		d := evaluatePerfGate(3*time.Millisecond, -43*time.Millisecond)
		require.True(t, d.AbsoluteOK)
		require.False(t, d.RatioApplicable)
		require.NotEmpty(t, d.RatioSkipReason)
	})

	t.Run("sub-floor job baseline skips the ratio", func(t *testing.T) {
		// 20ms is below the 50ms floor — less than a pod schedule plus container
		// boot can physically take, so it is treated as skew.
		d := evaluatePerfGate(3*time.Millisecond, 20*time.Millisecond)
		require.False(t, d.RatioApplicable)
		require.NotEmpty(t, d.RatioSkipReason)
	})

	t.Run("worker over one second fails the absolute bound regardless of the job", func(t *testing.T) {
		// The absolute bound is the hard gate and cannot be escaped by a plausible
		// or an implausible Job baseline.
		plausible := evaluatePerfGate(1500*time.Millisecond, 370*time.Millisecond)
		require.False(t, plausible.AbsoluteOK)
		implausible := evaluatePerfGate(1500*time.Millisecond, -43*time.Millisecond)
		require.False(t, implausible.AbsoluteOK)
	})
}
