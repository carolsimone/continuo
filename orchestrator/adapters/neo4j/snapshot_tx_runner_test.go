package neo4jinfra_test

import (
	"context"
	"errors"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

func TestSnapshotTxRunner_HappyPath(t *testing.T) {
	client := newTestClient(t) // skips if Neo4j unavailable
	runner := neo4jinfra.NewSnapshotTxRunner(client)
	called := false
	err := runner.Run(context.Background(), func(r snapshot.TopologyReader, w snapshot.SnapshotWriter) error {
		called = true
		if r == nil || w == nil {
			t.Error("reader or writer is nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !called {
		t.Error("fn was not called")
	}
}

func TestSnapshotTxRunner_FnErrorPropagates(t *testing.T) {
	client := newTestClient(t) // skips if Neo4j unavailable
	runner := neo4jinfra.NewSnapshotTxRunner(client)
	want := errors.New("boom")
	got := runner.Run(context.Background(), func(snapshot.TopologyReader, snapshot.SnapshotWriter) error {
		return want
	})
	if !errors.Is(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
