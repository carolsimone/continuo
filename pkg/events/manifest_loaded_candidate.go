package events

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// ManifestLoadedCandidateSchemaVersion is the schema_version of
// manifest.loaded.candidate:v2 entries.
const ManifestLoadedCandidateSchemaVersion = 1

// The outcomes a manifest.loaded.candidate:v2 entry reports.
const (
	ManifestStatusOK     = "ok"
	ManifestStatusFailed = "failed"
)

// ManifestFailedNode is one node topology-controller could not resolve.
type ManifestFailedNode struct {
	NodeID   string `json:"node_id"`
	Kind     string `json:"kind"`
	Service  string `json:"service"`
	FilePath string `json:"file_path"`
	NodeType string `json:"node_type"`
	Detail   string `json:"detail"`
}

// ManifestLoadedCandidate is the payload of a manifest.loaded.candidate:v2
// entry: how resolving one candidate release ended. An ok entry names the
// release's topology artifact (URI, SHA-256 of the stored object, node count),
// its code bundle and its contract bundle (URI and SHA-256 of the stored
// object); a failed entry carries the parse failure kind, a detail and the
// nodes that caused it. Both outcomes carry ContinuoContracts, the version of
// the continuo-contracts library that read the release's contracts. The
// topology itself is never on the stream. The contract fields are empty on an
// entry from a producer that does not write them.
type ManifestLoadedCandidate struct {
	ReleaseID            string               `json:"release_id"`
	Status               string               `json:"status"`
	TopologyURI          string               `json:"topology_uri,omitempty"`
	TopologySHA256       string               `json:"topology_sha256,omitempty"`
	NodeCount            int                  `json:"node_count,omitempty"`
	CodeBundleURI        string               `json:"code_bundle_uri,omitempty"`
	ContractBundleURI    string               `json:"contract_bundle_uri,omitempty"`
	ContractBundleSHA256 string               `json:"contract_bundle_sha256,omitempty"`
	ContinuoContracts    string               `json:"continuo_contracts,omitempty"`
	FailureKind          string               `json:"failure_kind,omitempty"`
	Detail               string               `json:"detail,omitempty"`
	FailedNodes          []ManifestFailedNode `json:"failed_nodes,omitempty"`
}

// ManifestLoadedCandidateEventID derives the event id of a release's
// manifest.loaded.candidate:v2 entry. A release resolves to one outcome, so a
// redelivered release.requested that publishes again repeats the id.
func ManifestLoadedCandidateEventID(tenantID, releaseID string) string {
	name := strings.Join([]string{"manifest.loaded.candidate", tenantID, releaseID}, "|")
	return uuid.NewSHA1(EventIDNamespace, []byte(name)).String()
}

// ManifestLoadedCandidateFields renders p as the Redis fields of one
// manifest.loaded.candidate:v2 entry. An ok payload carries only the success
// fields and a failed payload only the failure fields, with failed_nodes always
// a list: the shape topology-controller publishes. Both carry continuo_contracts.
func ManifestLoadedCandidateFields(tenantID, producer string, occurredAt time.Time, p ManifestLoadedCandidate) (map[string]any, error) {
	env := Envelope{
		EventID:       ManifestLoadedCandidateEventID(tenantID, p.ReleaseID),
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: ManifestLoadedCandidateSchemaVersion,
	}
	if p.Status == ManifestStatusOK {
		return env.Fields(map[string]any{
			"release_id":             p.ReleaseID,
			"status":                 p.Status,
			"topology_uri":           p.TopologyURI,
			"topology_sha256":        p.TopologySHA256,
			"node_count":             p.NodeCount,
			"code_bundle_uri":        p.CodeBundleURI,
			"contract_bundle_uri":    p.ContractBundleURI,
			"contract_bundle_sha256": p.ContractBundleSHA256,
			"continuo_contracts":     p.ContinuoContracts,
		})
	}
	failed := p.FailedNodes
	if failed == nil {
		failed = []ManifestFailedNode{}
	}
	return env.Fields(map[string]any{
		"release_id":         p.ReleaseID,
		"status":             p.Status,
		"failure_kind":       p.FailureKind,
		"continuo_contracts": p.ContinuoContracts,
		"detail":             p.Detail,
		"failed_nodes":       failed,
	})
}
