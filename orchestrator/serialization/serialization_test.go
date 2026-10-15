package serialization

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/event"
)

const goldenRemediationRequested = `{"event_id":"e","source":"remediation","release_id":"r","code_bundle_uri":"s3://b","classified_at":"2026-01-02T03:04:05Z","nodes":[{"node_id":"n","category":"compile","error_signature":"sig","reason":"why","error_excerpt":"exc","dbt_log_uri":"s3://l"}]}`

func TestRemediationRequestedRoundTrip(t *testing.T) {
	var dto RemediationRequestedDTO
	if err := json.Unmarshal([]byte(goldenRemediationRequested), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	out, err := json.Marshal(RemediationRequestedFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenRemediationRequested {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenRemediationRequested)
	}
	if got.EventID != "e" || len(got.Nodes) != 1 || got.Nodes[0].NodeID != "n" || got.Nodes[0].DBTLogURI != "s3://l" {
		t.Fatalf("toDomain fields wrong: %+v", got)
	}
}

const goldenPROpened = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n","n2"],"pr_url":"http://x","pr_number":5,"opened_by":"bot","opened_at":"2026-01-02T03:04:05Z","service":"svc"}`

func TestPROpenedRoundTrip(t *testing.T) {
	var dto PROpenedDTO
	if err := json.Unmarshal([]byte(goldenPROpened), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	out, err := json.Marshal(PROpenedFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenPROpened {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenPROpened)
	}
	if got.ProposalID != "p" || got.PrNumber != 5 || got.Service != "svc" {
		t.Fatalf("toDomain fields wrong: %+v", got)
	}
}

const goldenPRClosed = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n"],"service":"svc","pr_url":"http://x","pr_number":5,"outcome":"merged","closed_at":"2026-01-02T03:04:05Z","edits":[{"path":"models/m.sql","target_node_id":"n","amended":false,"diff":"d"}]}`

func TestPRClosedRoundTrip(t *testing.T) {
	var dto PRClosedDTO
	if err := json.Unmarshal([]byte(goldenPRClosed), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	out, err := json.Marshal(PRClosedFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenPRClosed {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenPRClosed)
	}
	if got.Outcome != "merged" || len(got.Edits) != 1 || got.Edits[0].Path != "models/m.sql" {
		t.Fatalf("toDomain fields wrong: %+v", got)
	}
}

// TestPRClosedServiceAndEditsOmitempty pins that service and edits are omitted
// when empty (legacy whole-proposal PR / rejected PR).
func TestPRClosedServiceAndEditsOmitempty(t *testing.T) {
	out, _ := json.Marshal(PRClosedFromDomain(event.PRClosed{ProposalID: "p", NodeID: "n", Outcome: "rejected"}))
	s := string(out)
	if strings.Contains(s, `"service"`) || strings.Contains(s, `"edits"`) {
		t.Fatalf("service/edits must be omitted when empty: %s", s)
	}
}
