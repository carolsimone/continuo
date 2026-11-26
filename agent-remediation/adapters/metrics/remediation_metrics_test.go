package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

// scrape returns what reg's /metrics handler serves.
func scrape(reg *pkgmetrics.Registry) string {
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func requireSeries(t *testing.T, body string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output lacks %q", w)
		}
	}
}

// Every counter carries the flow, the type and its own labels, and the project
// label the registry adds.
func TestRemediationMetrics_CountersCarryTheirLabels(t *testing.T) {
	reg := pkgmetrics.New("agent-remediation")
	m := NewRemediationMetrics(reg)

	m.Try("rejected-release", "dbt-compile", "passed")
	m.Try("rejected-release", "dbt-compile", "passed")
	m.Try("rejected-release", "dbt-validation", "gate_failed")
	m.Round("rejected-release", "dbt-compile", "revised")
	m.Round("rejected-release", "dbt-compile", "passed")
	m.Outcome("rejected-release", "dbt-validation", "handed_over", "tries_spent")
	m.Outcome("rejected-release", "dbt-compile", "fixed", "")
	m.Outcome("rejected-release", "none", "handed_over", "no_type")
	m.SystemFailure("rejected-release", "step_failures")
	m.Tokens("rejected-release", "dbt-compile", "in", 100)
	m.Tokens("rejected-release", "dbt-compile", "in", 50)
	m.Tokens("rejected-release", "dbt-compile", "out", 20)

	requireSeries(t, scrape(reg),
		`continuo_remediation_tries_total{flow="rejected-release",result="passed",service="agent-remediation",type="dbt-compile"} 2`,
		`continuo_remediation_tries_total{flow="rejected-release",result="gate_failed",service="agent-remediation",type="dbt-validation"} 1`,
		`continuo_remediation_rounds_total{flow="rejected-release",result="revised",service="agent-remediation",type="dbt-compile"} 1`,
		`continuo_remediation_rounds_total{flow="rejected-release",result="passed",service="agent-remediation",type="dbt-compile"} 1`,
		`continuo_remediation_outcomes_total{flow="rejected-release",outcome="handed_over",reason="tries_spent",service="agent-remediation",type="dbt-validation"} 1`,
		`continuo_remediation_outcomes_total{flow="rejected-release",outcome="fixed",reason="",service="agent-remediation",type="dbt-compile"} 1`,
		`continuo_remediation_outcomes_total{flow="rejected-release",outcome="handed_over",reason="no_type",service="agent-remediation",type="none"} 1`,
		`continuo_remediation_system_failures_total{flow="rejected-release",reason="step_failures",service="agent-remediation"} 1`,
		`continuo_remediation_model_tokens_total{direction="in",flow="rejected-release",service="agent-remediation",type="dbt-compile"} 150`,
		`continuo_remediation_model_tokens_total{direction="out",flow="rejected-release",service="agent-remediation",type="dbt-compile"} 20`,
	)
}

// The stage histogram observes seconds in the buckets from 0.1 s to an hour,
// and a flow-level stage has an empty type.
func TestRemediationMetrics_StageObservesSecondsInItsBuckets(t *testing.T) {
	reg := pkgmetrics.New("agent-remediation")
	m := NewRemediationMetrics(reg)

	m.Stage("rejected-release", "dbt-compile", "propose", 2*time.Second)
	m.Stage("rejected-release", "dbt-compile", "propose", 20*time.Second)
	m.Stage("rejected-release", "", "gate", 10*time.Minute)

	requireSeries(t, scrape(reg),
		`continuo_remediation_stage_seconds_bucket{flow="rejected-release",service="agent-remediation",stage="propose",type="dbt-compile",le="1"} 0`,
		`continuo_remediation_stage_seconds_bucket{flow="rejected-release",service="agent-remediation",stage="propose",type="dbt-compile",le="5"} 1`,
		`continuo_remediation_stage_seconds_bucket{flow="rejected-release",service="agent-remediation",stage="propose",type="dbt-compile",le="60"} 2`,
		`continuo_remediation_stage_seconds_count{flow="rejected-release",service="agent-remediation",stage="propose",type="dbt-compile"} 2`,
		`continuo_remediation_stage_seconds_sum{flow="rejected-release",service="agent-remediation",stage="propose",type="dbt-compile"} 22`,
		`continuo_remediation_stage_seconds_bucket{flow="rejected-release",service="agent-remediation",stage="gate",type="",le="900"} 1`,
		`continuo_remediation_stage_seconds_bucket{flow="rejected-release",service="agent-remediation",stage="gate",type="",le="3600"} 1`,
	)
}
