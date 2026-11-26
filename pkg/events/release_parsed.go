package events

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// ReleaseParsedSchemaVersion is the schema_version of
// release.parsed:v1 entries.
const ReleaseParsedSchemaVersion = 1

// The outcomes a release.parsed:v1 entry reports.
const (
	ManifestStatusOK     = "ok"
	ManifestStatusFailed = "failed"
)

// ManifestFailedNode is one node topology-controller could not resolve.
type ManifestFailedNode struct {
	NodeID   string `json:"node_id"`
	Kind     string `json:"kind"`
	Project  string `json:"project"`
	FilePath string `json:"file_path"`
	NodeType string `json:"node_type"`
	Detail   string `json:"detail"`
}

// ReleaseParsed is the payload of a release.parsed:v1
// entry: how resolving one candidate release ended. An ok entry names the
// release's topology artifact (URI, SHA-256 of the stored object, node count),
// its code bundle and its contract bundle (URI and SHA-256 of the stored
// object); a failed entry carries the parse failure kind, a detail and the
// nodes that caused it. Both outcomes carry ContinuoContracts, the version of
// the continuo-contracts library that read the release's contracts, and the two
// durations of the parse: ParseMs, the milliseconds from the start of the parse
// to the publish without the contract stages, and ContractsMs, the milliseconds
// of the contract stages (generating, checking, hashing and bundling the
// contracts). An ok entry also carries ContractsByFamily, the number of
// contracts its bundle holds by family (`dbt`, `python`); a family with none has
// no key. The topology itself is never on the stream. The contract fields and
// the durations are zero or empty on an entry from a producer that does not
// write them.
type ReleaseParsed struct {
	ReleaseID            string                       `json:"release_id"`
	Status               string                       `json:"status"`
	TopologyURI          string                       `json:"topology_uri,omitempty"`
	TopologySHA256       string                       `json:"topology_sha256,omitempty"`
	NodeCount            int                          `json:"node_count,omitempty"`
	CodeBundleURI        string                       `json:"code_bundle_uri,omitempty"`
	ContractBundleURI    string                       `json:"contract_bundle_uri,omitempty"`
	ContractBundleSHA256 string                       `json:"contract_bundle_sha256,omitempty"`
	ContinuoContracts    string                       `json:"continuo_contracts,omitempty"`
	ParseMs              int64                        `json:"parse_ms,omitempty"`
	ContractsMs          int64                        `json:"contracts_ms,omitempty"`
	ContractsByFamily    map[model.ContractFamily]int `json:"contracts_by_family,omitempty"`
	FailureKind          string                       `json:"failure_kind,omitempty"`
	Detail               string                       `json:"detail,omitempty"`
	FailedNodes          []ManifestFailedNode         `json:"failed_nodes,omitempty"`
}

// ReleaseParsedEventID derives the event id of a release's
// release.parsed:v1 entry. A release resolves to one outcome, so a
// redelivered release.requested that publishes again repeats the id.
func ReleaseParsedEventID(tenantID, releaseID string) string {
	name := strings.Join([]string{"release.parsed", tenantID, releaseID}, "|")
	return uuid.NewSHA1(EventIDNamespace, []byte(name)).String()
}

// ReleaseParsedFields renders p as the Redis fields of one
// release.parsed:v1 entry. An ok payload carries only the success
// fields and a failed payload only the failure fields, with failed_nodes always
// a list: the shape topology-controller publishes. Both carry continuo_contracts,
// parse_ms and contracts_ms; an ok payload also carries contracts_by_family,
// always an object.
func ReleaseParsedFields(tenantID, producer string, occurredAt time.Time, p ReleaseParsed) (map[string]any, error) {
	env := Envelope{
		EventID:       ReleaseParsedEventID(tenantID, p.ReleaseID),
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: ReleaseParsedSchemaVersion,
	}
	if p.Status == ManifestStatusOK {
		byFamily := p.ContractsByFamily
		if byFamily == nil {
			byFamily = map[model.ContractFamily]int{}
		}
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
			"parse_ms":               p.ParseMs,
			"contracts_ms":           p.ContractsMs,
			"contracts_by_family":    byFamily,
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
		"parse_ms":           p.ParseMs,
		"contracts_ms":       p.ContractsMs,
		"detail":             p.Detail,
		"failed_nodes":       failed,
	})
}
