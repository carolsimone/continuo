package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestExpireOnce_DeletesOnlyPastTheHorizon(t *testing.T) {
	h := newHarness()
	old := storedOpen(h.repo, deadletter.SourceConsumer, "g", now.Add(-model.ReplayHorizon-time.Second))
	fresh := storedOpen(h.repo, deadletter.SourceConsumer, "g", now.Add(-model.ReplayHorizon+time.Hour))
	n, err := h.expirer.ExpireOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if h.repo.has(old.ID) || !h.repo.has(fresh.ID) || len(h.obs.expired) != 1 {
		t.Fatal("wrong rows expired")
	}
}
