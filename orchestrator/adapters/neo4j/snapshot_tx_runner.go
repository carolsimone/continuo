package neo4jinfra

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// SnapshotTxRunner implements snapshot.TxRunner. It opens a Neo4j write session,
// starts a managed write transaction, and hands the caller a tx-bound
// TopologyReader + SnapshotWriter. The two ports share the same
// neo4j.ManagedTransaction so reads and the subsequent write commit atomically.
type SnapshotTxRunner struct {
	Client Neo4jClient
}

// NewSnapshotTxRunner constructs a SnapshotTxRunner.
func NewSnapshotTxRunner(client Neo4jClient) *SnapshotTxRunner {
	return &SnapshotTxRunner{Client: client}
}

// Run opens a single ExecuteWrite, builds the tx-bound reader and writer, and
// invokes fn. If fn returns an error the transaction is rolled back; otherwise
// it commits.
func (s *SnapshotTxRunner) Run(ctx context.Context, fn func(snapshot.TopologyReader, snapshot.SnapshotWriter) error) error {
	session := s.Client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r := newTopologyReader(tx)
		w := newSnapshotWriter(tx)
		return nil, fn(r, w)
	})
	return err
}
