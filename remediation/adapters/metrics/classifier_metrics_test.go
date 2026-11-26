package metrics

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

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

// Each counted decision lands on the series of its decision and its rule's
// family, next to the component label the registry adds.
func TestClassifierMetrics_CountsEachNewDecision(t *testing.T) {
	reg := pkgmetrics.New("remediation")
	m := NewClassifierMetrics(reg)

	m.Classified("emit", "logic:missing_object")
	m.Classified("emit", "logic:syntax_error")
	m.Classified("emit", "test:assertion_failed")
	m.Classified("emit", "unknown:unmatched")
	m.Classified("drop", "infra:connection_refused")
	m.Classified("drop", "infra:oomkilled")
	m.Classified("drop", "parse:not_healable")

	requireSeries(t, scrape(reg),
		`continuo_remediation_classified_total{component="remediation",decision="emit",reason="logic"} 2`,
		`continuo_remediation_classified_total{component="remediation",decision="emit",reason="test"} 1`,
		`continuo_remediation_classified_total{component="remediation",decision="emit",reason="unknown"} 1`,
		`continuo_remediation_classified_total{component="remediation",decision="drop",reason="infra"} 2`,
		`continuo_remediation_classified_total{component="remediation",decision="drop",reason="parse"} 1`,
	)
}

// The reason label holds the rule's family and nothing after the first colon,
// so a rule name that carries free text cannot add a series.
func TestClassifierMetrics_ReasonKeepsOnlyTheRuleFamily(t *testing.T) {
	reg := pkgmetrics.New("remediation")
	m := NewClassifierMetrics(reg)

	m.Classified("emit", `logic:relation "analytics.orders_7f3a" does not exist: line 12`)
	m.Classified("emit", `logic:column "custmer_id" does not exist`)
	m.Classified("drop", "infra:")
	m.Classified("emit", "unknown")

	body := scrape(reg)
	requireSeries(t, body,
		`continuo_remediation_classified_total{component="remediation",decision="emit",reason="logic"} 2`,
		`continuo_remediation_classified_total{component="remediation",decision="drop",reason="infra"} 1`,
		`continuo_remediation_classified_total{component="remediation",decision="emit",reason="unknown"} 1`,
	)
	series := regexp.MustCompile(`(?m)^continuo_remediation_classified_total\{.*$`).FindAllString(body, -1)
	if len(series) != 3 {
		t.Fatalf("got %d series, want 3 (one per decision and family): %q", len(series), series)
	}
	for _, line := range series {
		if strings.Contains(line, "orders_7f3a") || strings.Contains(line, "custmer_id") || strings.Contains(line, "line 12") {
			t.Errorf("a free-text detail reached a label: %s", line)
		}
	}
}
