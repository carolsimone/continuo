package serialization

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
)

const goldenPROpened = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n"],"service":"svc","pr_url":"http://x","pr_number":5,"opened_by":"bot","opened_at":"2026-01-02T03:04:05Z"}`

func TestPROpenedRoundTrip(t *testing.T) {
	var dto PROpenedDTO
	if err := json.Unmarshal([]byte(goldenPROpened), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	out, err := json.Marshal(PROpenedFromDomain(dto.ToDomain()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenPROpened {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenPROpened)
	}
	if dto.ToDomain().PrNumber != 5 || dto.ToDomain().Service != "svc" {
		t.Fatalf("toDomain fields wrong: %+v", dto.ToDomain())
	}
}

func TestPROpenedServiceOmitempty(t *testing.T) {
	out, _ := json.Marshal(PROpenedFromDomain(event.PROpened{ProposalID: "p"}))
	if strings.Contains(string(out), `"service"`) {
		t.Fatalf("service must be omitted when empty: %s", out)
	}
}

const goldenPRClosed = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n"],"service":"svc","pr_url":"http://x","pr_number":5,"outcome":"merged","closed_at":"2026-01-02T03:04:05Z","edits":[{"path":"models/m.sql","target_node_id":"n","amended":true,"diff":"d"}]}`

func TestPRClosedRoundTrip(t *testing.T) {
	var dto PRClosedDTO
	if err := json.Unmarshal([]byte(goldenPRClosed), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	out, err := json.Marshal(PRClosedFromDomain(dto.ToDomain()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenPRClosed {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenPRClosed)
	}
	d := dto.ToDomain()
	if d.Outcome != "merged" || len(d.Edits) != 1 || !d.Edits[0].Amended {
		t.Fatalf("toDomain fields wrong: %+v", d)
	}
}

func TestPRClosedServiceAndEditsOmitempty(t *testing.T) {
	out, _ := json.Marshal(PRClosedFromDomain(event.PRClosed{ProposalID: "p", Outcome: "rejected"}))
	s := string(out)
	if strings.Contains(s, `"service"`) || strings.Contains(s, `"edits"`) {
		t.Fatalf("service/edits must be omitted when empty: %s", s)
	}
}

const goldenRemediationProposed = `{"event_id":"e","source":"agent","release_id":"r","remediation_round":1,"node_id":"n","resolved_node_ids":["n"],"error_signature":"sig","proposed_sql_uri":"s3://sql","diff_uri":"s3://diff","edits":[{"path":"models/m.sql","content_uri":"s3://c","diff_uri":"s3://d","target_node_id":"n"}],"rationale":"why","confidence":"high","model":"claude","attempt":1,"source_resolved":true,"proposed_at":"2026-01-02T03:04:05Z"}`

func TestRemediationProposedRoundTrip(t *testing.T) {
	var dto RemediationProposedDTO
	if err := json.Unmarshal([]byte(goldenRemediationProposed), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	out, err := json.Marshal(RemediationProposedFromDomain(dto.ToDomain()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenRemediationProposed {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenRemediationProposed)
	}
	d := dto.ToDomain()
	if d.EventID != "e" || len(d.Edits) != 1 || d.Edits[0].ContentURI != "s3://c" || !d.SourceResolved {
		t.Fatalf("toDomain fields wrong: %+v", d)
	}
	// The trigger names no free-text guess at a single root-cause node: each edit
	// says which node it changes (target_node_id), which replaced the guess.
	if strings.Contains(string(out), "suspected_root_cause") {
		t.Fatalf("wire shape must not carry a suspected_root_cause field: %s", out)
	}
}
