package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
)

func TestQueryList_NormalizesAndCounts(t *testing.T) {
	h := newHarness()
	storedOpen(h.repo, deadletter.SourceConsumer, "g", now)
	got, total, err := h.query.List(context.Background(), deadletter.Filter{})
	if err != nil || len(got) != 1 || total != 1 {
		t.Fatalf("got=%d total=%d err=%v", len(got), total, err)
	}
	if h.repo.lastFilter.Limit != 50 || h.repo.lastFilter.Status != deadletter.StatusOpen {
		t.Fatalf("filter not normalized: %+v", h.repo.lastFilter)
	}
	if _, _, err := h.query.List(context.Background(), deadletter.Filter{Source: "x"}); !errors.Is(err, deadletter.ErrInvalidFilter) {
		t.Fatalf("err = %v", err)
	}
}
