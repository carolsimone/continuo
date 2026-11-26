// Package metrics holds agent-remediation's own Prometheus series.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

// RemediationMetrics counts what the remediation runner does, by the flow it
// follows and the type of the cluster it works: how each cluster ended in a
// try, how each pre-flight pass ended, how each node ended when its remediation
// did, the remediations that stopped for a failure of the runner, how long each
// committed step took, and the tokens the model used. A series exists once its
// first sample is counted, since the flows and types are the install's own
// files.
type RemediationMetrics struct {
	tries          *prometheus.CounterVec
	rounds         *prometheus.CounterVec
	outcomes       *prometheus.CounterVec
	systemFailures *prometheus.CounterVec
	stageSeconds   *prometheus.HistogramVec
	tokens         *prometheus.CounterVec
}

var _ ports.RemediationMetrics = (*RemediationMetrics)(nil)

// NewRemediationMetrics registers continuo_remediation_tries_total,
// continuo_remediation_rounds_total, continuo_remediation_outcomes_total,
// continuo_remediation_system_failures_total,
// continuo_remediation_stage_seconds and
// continuo_remediation_model_tokens_total on reg.
func NewRemediationMetrics(reg *pkgmetrics.Registry) *RemediationMetrics {
	m := &RemediationMetrics{
		tries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_remediation_tries_total",
			Help: "Clusters a remediation try worked, counted when the try ends, by flow, type and result: passed (the release checks passed), gate_failed (a run did not pass the fix or never answered), checks_failed (a cross-cluster check or the gate refused the try's drafts), preflight_spent (every pre-flight round refused the draft) or declined (a step declined, or the draft made no edit). A cluster no type matches and one handed over when the try opened count nothing, and so does a shared-upstream cluster that fell back to its members, which count their own result. A cross-project cluster that fell back counts declined or preflight_spent, the result of its own failure.",
		}, []string{"flow", "type", "result"}),
		rounds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_remediation_rounds_total",
			Help: "Pre-flight passes of a cluster's draft, by flow, type and result: passed (no check found anything), revised (a check found something and the model revises the draft in the next round) or spent (a check found something in the type's last round).",
		}, []string{"flow", "type", "result"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_remediation_outcomes_total",
			Help: "Failing nodes of remediations that ended, by flow, the type of the node's cluster (none when no type matched), outcome and reason: fixed (a verified fix was handed over for review, with no reason) or handed_over (the node was left to a human, the reason being the code it was handed over with).",
		}, []string{"flow", "type", "outcome", "reason"}),
		systemFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_remediation_system_failures_total",
			Help: "Remediations the runner stopped because they could go no further, by flow and reason: step_failures (a step failed five times), remediation_failures (the remediation failed five times in a row before a step could run) or deadline (a drafting or checking try ran past its deadline). A stopped remediation waits for a redrive of its dead-lettered trigger, and counts again if it stops again.",
		}, []string{"flow", "reason"}),
		stageSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "continuo_remediation_stage_seconds",
			Help:    "Seconds each committed step of a remediation took, by flow, type and stage: context, propose, preflight, cross_cluster, gate or output. The steps of a cluster, the flow's own context steps among them, count under the cluster's type, and the cross_cluster, gate and output stages have an empty type. A gate stage lasts from the submit of the try's drafts to the verdict.",
			Buckets: []float64{0.1, 0.5, 1, 5, 15, 60, 300, 900, 3600},
		}, []string{"flow", "type", "stage"}),
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_remediation_model_tokens_total",
			Help: "Tokens the model used for remediation, by flow, type and direction (in for the prompt, out for the answer). An answer replayed from the response cache uses none.",
		}, []string{"flow", "type", "direction"}),
	}
	reg.Register(m.tries, m.rounds, m.outcomes, m.systemFailures, m.stageSeconds, m.tokens)
	return m
}

// Try counts one cluster ended by a try with result.
func (m *RemediationMetrics) Try(flow, typ, result string) {
	m.tries.WithLabelValues(flow, typ, result).Inc()
}

// Round counts one pre-flight pass with result.
func (m *RemediationMetrics) Round(flow, typ, result string) {
	m.rounds.WithLabelValues(flow, typ, result).Inc()
}

// Outcome counts one node of a remediation that ended.
func (m *RemediationMetrics) Outcome(flow, typ, outcome, reason string) {
	m.outcomes.WithLabelValues(flow, typ, outcome, reason).Inc()
}

// SystemFailure counts one remediation stopped for reason.
func (m *RemediationMetrics) SystemFailure(flow, reason string) {
	m.systemFailures.WithLabelValues(flow, reason).Inc()
}

// Stage observes the seconds a committed step took.
func (m *RemediationMetrics) Stage(flow, typ, stage string, d time.Duration) {
	m.stageSeconds.WithLabelValues(flow, typ, stage).Observe(d.Seconds())
}

// Tokens adds n tokens in direction.
func (m *RemediationMetrics) Tokens(flow, typ, direction string, n int) {
	m.tokens.WithLabelValues(flow, typ, direction).Add(float64(n))
}
