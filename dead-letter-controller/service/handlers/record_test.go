package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestRecord_StoresOnceAndObservesOnce(t *testing.T) {
	h := newHarness()
	dl := deadletter.DeadLetter{DedupKey: "consumer|s|g|1-0", Source: deadletter.SourceConsumer, FailureKind: model.DeadLetterKindPermanent,
		Stream: "s", Group: "g", Fields: map[string]string{"k": "v"}, Redrivable: true, OriginalAt: now, RecordedAt: now, Status: deadletter.StatusOpen}
	for i := 0; i < 2; i++ {
		if err := h.recorder.Record(context.Background(), dl); err != nil {
			t.Fatal(err)
		}
	}
	if h.repo.count() != 1 || len(h.obs.recorded) != 1 {
		t.Fatalf("rows=%d observed=%d", h.repo.count(), len(h.obs.recorded))
	}
}

func TestRecord_StoreFailureIsReturned(t *testing.T) {
	h := newHarness()
	h.repo.insertErr = errors.New("connection refused")
	err := h.recorder.Record(context.Background(), deadletter.DeadLetter{DedupKey: "k", Status: deadletter.StatusOpen})
	if err == nil {
		t.Fatal("a store failure must reach the consumer so the entry stays pending")
	}
}
