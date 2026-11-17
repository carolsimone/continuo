// Package metrics holds remediation's own Prometheus series.
package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/remediation/service/ports"
)

// ClassifierMetrics counts the failure classifier's decisions, by the decision
// and the family of the rule that matched. A series exists once its first
// decision is counted.
type ClassifierMetrics struct {
	classified *prometheus.CounterVec
}

var _ ports.ClassifierMetrics = (*ClassifierMetrics)(nil)

// NewClassifierMetrics registers continuo_remediation_classified_total on reg.
func NewClassifierMetrics(reg *pkgmetrics.Registry) *ClassifierMetrics {
	m := &ClassifierMetrics{
		classified: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_remediation_classified_total",
			Help: "Failing nodes the classifier recorded a decision for, by decision and reason: emit (a change to the release's source can fix the node) or drop (none can, so the node is handed over unfixable), and the family of the rule that matched: infra, parse, logic, test or unknown. A node already recorded for its release and round is not counted again.",
		}, []string{"decision", "reason"}),
	}
	reg.Register(m.classified)
	return m
}

// Classified counts one decision. The reason label is the rule up to its first
// colon, so the detail a rule name carries after it stays out of the labels.
func (m *ClassifierMetrics) Classified(decision, rule string) {
	family, _, _ := strings.Cut(rule, ":")
	m.classified.WithLabelValues(decision, family).Inc()
}
