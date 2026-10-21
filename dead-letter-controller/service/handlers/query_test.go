package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/google/uuid"
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

func TestQuery_ShowsWhyADeadLetterCannotBeRedriven(t *testing.T) {
	h := newHarness()
	live := storedOpen(h.repo, deadletter.SourceConsumer, "g", now)
	retired := storedOpen(h.repo, deadletter.SourceConsumer, "g", now)
	retired.Stream = "orders.created:v1"
	h.repo.put(retired)

	got, _, err := h.query.List(context.Background(), deadletter.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	refusals := map[uuid.UUID]string{}
	for _, v := range got {
		refusals[v.ID] = v.Refusal
	}
	if refusals[live.ID] != "" || refusals[retired.ID] != "stream orders.created:v1 is retired" {
		t.Fatalf("refusals = %v", refusals)
	}

	one, err := h.query.Get(context.Background(), retired.ID)
	if err != nil || one.Refusal != "stream orders.created:v1 is retired" {
		t.Fatalf("get = %+v, %v", one, err)
	}
}
