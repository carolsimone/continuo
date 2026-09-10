// Package serialization holds the wire-facing DTOs for state's json-tagged
// domain events, keeping the domain events package free of struct tags. Only the
// redis parser serializes these types, so it lives in the adapters tree; the
// parser maps decoded payloads through it, fixing the release.seeds.pending:v1
// byte shape here.
package serialization

import (
	"github.com/carolsimone/continuo/state/domain/events"
)

// SeedNodeDTO is the JSON shape of one seed in a release.seeds.pending:v1 payload.
type SeedNodeDTO struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	NodeType    string `json:"node_type"`
	ImageTag    string `json:"image_tag"`
}

// ReleaseSeedsPendingDTO is the JSON shape of the release.seeds.pending:v1 payload.
type ReleaseSeedsPendingDTO struct {
	ReleaseID string        `json:"release_id"`
	Nodes     []SeedNodeDTO `json:"nodes"`
}

// ToDomain maps a decoded DTO back to the domain event.
func (d ReleaseSeedsPendingDTO) ToDomain() events.ReleaseSeedsPending {
	var nodes []events.SeedNode
	if d.Nodes != nil {
		nodes = make([]events.SeedNode, len(d.Nodes))
		for i, n := range d.Nodes {
			nodes[i] = events.SeedNode(n)
		}
	}
	return events.ReleaseSeedsPending{ReleaseID: d.ReleaseID, Nodes: nodes}
}
