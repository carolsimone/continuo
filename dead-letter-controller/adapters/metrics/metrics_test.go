package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

type backlog []deadletter.BacklogRow

func (b backlog) Backlog(context.Context) ([]deadletter.BacklogRow, error) { return b, nil }

func scrape(reg *pkgmetrics.Registry) string {
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

func TestMetrics_CountersAndBacklog(t *testing.T) {
	reg := pkgmetrics.New("dead-letter-controller")
	m := New(reg, backlog{{Source: deadletter.SourceConsumer, Stream: "s:v1", Kind: model.DeadLetterKindPermanent,
		Open: 4, OldestRecordedAt: time.Now().Add(-time.Minute)}})
	m.Recorded(deadletter.DeadLetter{Source: deadletter.SourceConsumer, Stream: "s:v1", Group: "g", FailureKind: model.DeadLetterKindPermanent})
	m.Redriven(deadletter.DeadLetter{Source: deadletter.SourceConsumer, Stream: "s:v1"})
	m.Expired(deadletter.DeadLetter{Source: deadletter.SourceOutbox})
	body := scrape(reg)
	for _, want := range []string{
		`continuo_dead_letters_total{group="g",kind="permanent",service="dead-letter-controller",source="consumer",stream="s:v1"} 1`,
		`continuo_dead_letters_redriven_total{service="dead-letter-controller",source="consumer",stream="s:v1"} 1`,
		`continuo_dead_letters_expired_total{service="dead-letter-controller",source="outbox"} 1`,
		`continuo_dead_letter_backlog{kind="permanent",service="dead-letter-controller",source="consumer",stream="s:v1"} 4`,
		`continuo_dead_letter_oldest_open_age_seconds{service="dead-letter-controller",source="consumer"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
}
