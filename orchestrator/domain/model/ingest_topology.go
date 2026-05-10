package model

import "github.com/carolsimone/continuo/orchestrator/domain/event"

// IngestTopologyInput is the handler-input wrapper for a topology
// ingestion operation. The inner []event.ManifestLoadedNode slice is the
// wire-format payload (unmarshalled directly from manifest.loaded:v1);
// this outer struct is built by the consumer after that unmarshal.
type IngestTopologyInput struct {
	Nodes []event.ManifestLoadedNode
}
