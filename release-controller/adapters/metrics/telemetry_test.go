package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelemetry_ExportsEveryOutcomeAndRejectReasonAtZero(t *testing.T) {
	tel := NewTelemetry(pkgmetrics.New("release-controller"))
	require.Equal(t, 1+len(model.RejectReasons()), testutil.CollectAndCount(tel.releases))
	assert.Equal(t, 0.0, testutil.ToFloat64(tel.releases.WithLabelValues("promoted", "none")))
	for _, reason := range model.RejectReasons() {
		assert.Equal(t, 0.0, testutil.ToFloat64(tel.releases.WithLabelValues("rejected", string(reason))))
	}
}

func TestTelemetry_RecordsStageDurationsAndReleaseOutcomes(t *testing.T) {
	reg := pkgmetrics.New("release-controller")
	tel := NewTelemetry(reg)
	ctx := context.Background()
	tel.ReleaseCompileCompleted(ctx, "rel-1", true, 30*time.Second)
	tel.ReleaseParseCompleted(ctx, "rel-1", false, 9*time.Second)
	tel.ReleasePromoted(ctx, "rel-2", 3)
	tel.ReleaseRejected(ctx, "rel-1", string(model.RejectReasonInvalidSQL), nil)
	assert.Equal(t, 1.0, testutil.ToFloat64(tel.releases.WithLabelValues("promoted", "none")))
	assert.Equal(t, 1.0, testutil.ToFloat64(tel.releases.WithLabelValues("rejected", "invalid_sql")))
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Contains(t, rec.Body.String(), `continuo_release_stage_duration_seconds_sum{outcome="failed",service="release-controller",stage="parsing"} 9`)
	assert.Contains(t, rec.Body.String(), `continuo_release_stage_duration_seconds_count{outcome="ok",service="release-controller",stage="compiling"} 1`)
}
