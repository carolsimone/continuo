package serialization

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/catalog/domain/event"
)

const goldenPROpened = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n","n2"],"pr_url":"http://x","pr_number":5,"opened_by":"bot","opened_at":"2026-01-02T03:04:05Z","project":"svc"}`

func TestPROpenedDecodes(t *testing.T) {
	var dto PROpenedDTO
	if err := json.Unmarshal([]byte(goldenPROpened), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	want := event.PROpened{
		ProposalID: "p", ReleaseID: "r", NodeID: "n", ResolvedNodeIDs: []string{"n", "n2"},
		PrURL: "http://x", PrNumber: 5, OpenedBy: "bot", OpenedAt: "2026-01-02T03:04:05Z", Project: "svc",
	}
	if got := dto.ToDomain(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ToDomain:\n got %+v\nwant %+v", got, want)
	}
}

const goldenPRClosed = `{"proposal_id":"p","release_id":"r","node_id":"n","resolved_node_ids":["n"],"project":"svc","pr_url":"http://x","pr_number":5,"outcome":"merged","closed_at":"2026-01-02T03:04:05Z","edits":[{"path":"models/m.sql","target_node_id":"n","amended":false,"diff":"d"}]}`

func TestPRClosedDecodes(t *testing.T) {
	var dto PRClosedDTO
	if err := json.Unmarshal([]byte(goldenPRClosed), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	want := event.PRClosed{
		ProposalID: "p", ReleaseID: "r", NodeID: "n", ResolvedNodeIDs: []string{"n"}, Project: "svc",
		PrURL: "http://x", PrNumber: 5, Outcome: "merged", ClosedAt: "2026-01-02T03:04:05Z",
		Edits: []event.PRClosedEdit{{Path: "models/m.sql", TargetNodeID: "n", Amended: false, Diff: "d"}},
	}
	if got := dto.ToDomain(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ToDomain:\n got %+v\nwant %+v", got, want)
	}
}
