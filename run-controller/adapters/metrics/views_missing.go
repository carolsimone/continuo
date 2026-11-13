package metrics

import (
	"context"
	"log/slog"
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/prometheus/client_golang/prometheus"
)

// viewsMissingReadTimeout bounds the read of the outages at each scrape.
const viewsMissingReadTimeout = 2 * time.Second

// ViewOutageCounter counts the open view outages of every tenant by state.
type ViewOutageCounter interface {
	CountByState(ctx context.Context) (map[seeds.OutageState]int, error)
}

// viewsMissingCollector exports continuo_views_missing{state}, read from the
// open view outages at each scrape.
type viewsMissingCollector struct {
	counts ViewOutageCounter
	logger *slog.Logger
	desc   *prometheus.Desc
}

// NewViewsMissingCollector registers continuo_views_missing on reg. Each scrape
// reads counts under a 2 s timeout and exports both states, a state with no
// outage at 0; a read that fails exports no sample and logs at WARN.
func NewViewsMissingCollector(reg *pkgmetrics.Registry, counts ViewOutageCounter, logger *slog.Logger) {
	reg.Register(&viewsMissingCollector{
		counts: counts,
		logger: logger,
		desc: prometheus.NewDesc("continuo_views_missing",
			"Views a seed load dropped (missing) or left stale (stale) whose rebuild has not yet succeeded, of every tenant, read when scraped.",
			[]string{"state"}, nil),
	})
}

func (c *viewsMissingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *viewsMissingCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), viewsMissingReadTimeout)
	defer cancel()
	counts, err := c.counts.CountByState(ctx)
	if err != nil {
		c.logger.Warn("metrics: could not read the view outages; continuo_views_missing is left out of this scrape", "error", err)
		return
	}
	for _, state := range seeds.OutageStates() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(counts[state]), string(state))
	}
}
