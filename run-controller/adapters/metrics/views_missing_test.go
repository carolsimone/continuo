package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
)

// countsOf is a ViewOutageCounter that answers counts, or err.
type countsOf struct {
	counts map[seeds.OutageState]int
	err    error
}

func (c countsOf) CountByState(context.Context) (map[seeds.OutageState]int, error) {
	return c.counts, c.err
}

// Each scrape reads the outages and exports both states, a state with no
// outage at 0; a read that fails exports no sample and fails nothing else.
func TestViewsMissingCollector_ReportsBothStatesAtScrape(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
	NewViewsMissingCollector(reg, countsOf{counts: map[seeds.OutageState]int{seeds.OutageMissing: 2}}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	body := scrape(reg)
	for _, want := range []string{
		`continuo_views_missing{service="run-controller",state="missing"} 2`,
		`continuo_views_missing{service="run-controller",state="stale"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}

	failing := pkgmetrics.New("run-controller")
	NewViewsMissingCollector(failing, countsOf{err: errors.New("connection refused")}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	body = scrape(failing)
	if strings.Contains(body, "continuo_views_missing{") {
		t.Errorf("a failed read exports a sample:\n%s", body)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Errorf("a failed read fails the rest of the scrape:\n%s", body)
	}
}
