package metrics

import (
	"strings"
	"testing"
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

func TestTrimMetrics(t *testing.T) {
	reg := pkgmetrics.New("dead-letter-controller")
	dl := New(reg, backlog{})
	tm := NewTrim(reg, dl)
	tm.Quarantined("s:v1", "g", 3)
	tm.Trimmed("s:v1", 7)
	tm.TrimSucceeded(time.Unix(1700000000, 0))
	body := scrape(reg)
	for _, want := range []string{
		`continuo_stream_quarantined_total{group="g",service="dead-letter-controller",stream="s:v1"} 3`,
		`continuo_stream_trimmed_entries_total{service="dead-letter-controller",stream="s:v1"} 7`,
		`continuo_stream_trim_last_success_timestamp_seconds{service="dead-letter-controller"} 1.7e+09`,
		`continuo_dead_letters_total{group="g",kind="trimmed",service="dead-letter-controller",source="quarantine",stream="s:v1"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s\n%s", want, body)
		}
	}
}
