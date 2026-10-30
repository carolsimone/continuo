package serialization

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/catalog/domain/event"
)

const goldenRemediationRequested = `{"event_id":"e","source":"remediation","release_id":"r","code_bundle_uri":"s3://b","classified_at":"2026-01-02T03:04:05Z","nodes":[{"node_id":"n","category":"compile","error_signature":"sig","reason":"why","error_excerpt":"exc","dbt_log_uri":"s3://l"}]}`

func TestRemediationRequestedDecodes(t *testing.T) {
	var dto RemediationRequestedDTO
	if err := json.Unmarshal([]byte(goldenRemediationRequested), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	want := event.RemediationRequested{
		EventID: "e", Source: "remediation", ReleaseID: "r", CodeBundleURI: "s3://b",
		ClassifiedAt: "2026-01-02T03:04:05Z",
		Nodes: []event.RemediationRequestedNode{{
			NodeID: "n", Category: "compile", ErrorSignature: "sig", Reason: "why",
			ErrorExcerpt: "exc", DBTLogURI: "s3://l",
		}},
	}
	if got := dto.ToDomain(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ToDomain:\n got %+v\nwant %+v", got, want)
	}
}

const goldenPROpened = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n","n2"],"pr_url":"http://x","pr_number":5,"opened_by":"bot","opened_at":"2026-01-02T03:04:05Z","service":"svc"}`

func TestPROpenedDecodes(t *testing.T) {
	var dto PROpenedDTO
	if err := json.Unmarshal([]byte(goldenPROpened), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	want := event.PROpened{
		ProposalID: "p", ReleaseID: "r", NodeID: "n", ResolvedNodeIDs: []string{"n", "n2"},
		PrURL: "http://x", PrNumber: 5, OpenedBy: "bot", OpenedAt: "2026-01-02T03:04:05Z", Service: "svc",
	}
	if got := dto.ToDomain(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ToDomain:\n got %+v\nwant %+v", got, want)
	}
}

const goldenPRClosed = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n"],"service":"svc","pr_url":"http://x","pr_number":5,"outcome":"merged","closed_at":"2026-01-02T03:04:05Z","edits":[{"path":"models/m.sql","target_node_id":"n","amended":false,"diff":"d"}]}`

func TestPRClosedDecodes(t *testing.T) {
	var dto PRClosedDTO
	if err := json.Unmarshal([]byte(goldenPRClosed), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	want := event.PRClosed{
		ProposalID: "p", ReleaseID: "r", NodeID: "n", ResolvedNodeIDs: []string{"n"}, Service: "svc",
		PrURL: "http://x", PrNumber: 5, Outcome: "merged", ClosedAt: "2026-01-02T03:04:05Z",
		Edits: []event.PRClosedEdit{{Path: "models/m.sql", TargetNodeID: "n", Amended: false, Diff: "d"}},
	}
	if got := dto.ToDomain(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ToDomain:\n got %+v\nwant %+v", got, want)
	}
}
