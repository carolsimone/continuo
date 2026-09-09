package serialization

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/event"
)

const goldenNodeReady = `{"schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","task_id":"t","job_name":"j","node_type":"dbt-model","manifest_version":"v1","image_tag":"img","operation":"test"}`

func TestNodeReadyForExecutionRoundTrip(t *testing.T) {
	var dto NodeReadyForExecutionDTO
	if err := json.Unmarshal([]byte(goldenNodeReady), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := domain.NodeReadyForExecution{
		ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc", SchemaName: "sch",
		TableName: "tbl", TaskID: "t", JobName: "j", NodeType: "dbt-model",
		ManifestVersion: "v1", ImageTag: "img", Operation: "test",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(NodeReadyForExecutionFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenNodeReady {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenNodeReady)
	}
}

func TestNodeReadyOperationOmitempty(t *testing.T) {
	out, _ := json.Marshal(NodeReadyForExecutionFromDomain(domain.NodeReadyForExecution{TaskID: "t"}))
	if strings.Contains(string(out), `"operation"`) {
		t.Fatalf("operation must be omitted when empty: %s", out)
	}
}

const goldenReleasePromoted = `{"release_id":"r","topology":[{"unique_id":"svc.m","schema_name":"sch","table_name":"tbl","service_name":"svc","node_type":"dbt-model","content_hash":"h","test_count":2,"image_tag":"img","schedule":"daily","upstream_unique_ids":["svc.u"],"changed":true,"original_file_path":"models/m.sql"}],"image_tags":{"svc":"img"},"repo":"acme/demo","commit_sha":"abc","promoted_at":"2026-01-02T03:04:05Z","code_bundle_uri":"s3://b","bootstrap":false}`

func TestReleasePromotedRoundTrip(t *testing.T) {
	var dto ReleasePromotedDTO
	if err := json.Unmarshal([]byte(goldenReleasePromoted), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	// Byte shape: re-marshalling the decoded DTO reproduces the payload.
	out, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenReleasePromoted {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenReleasePromoted)
	}
	got := dto.ToDomain()
	want := event.ReleasePromoted{
		ReleaseID: "r",
		Topology: []event.ReleasePromotedNode{{
			UniqueID: "svc.m", SchemaName: "sch", TableName: "tbl", ServiceName: "svc",
			NodeType: "dbt-model", ContentHash: "h", TestCount: 2, ImageTag: "img",
			Schedule: "daily", UpstreamUniqueIDs: []string{"svc.u"}, Changed: true,
			OriginalFilePath: "models/m.sql",
		}},
		ImageTags: map[string]string{"svc": "img"}, Repo: "acme/demo", CommitSHA: "abc",
		PromotedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), CodeBundleURI: "s3://b", Bootstrap: false,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
}

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

const goldenPromotedSeedsNodes = `[{"service_name":"svc","schema_name":"sch","table_name":"tbl","node_type":"dbt-seed","image_tag":"img"}]`

func TestPromotedSeedsNodesRoundTrip(t *testing.T) {
	var dto []PromotedSeedsNodeDTO
	if err := json.Unmarshal([]byte(goldenPromotedSeedsNodes), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	out, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenPromotedSeedsNodes {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenPromotedSeedsNodes)
	}
	got := PromotedSeedsNodesToDomain(dto)
	if len(got) != 1 || got[0].ServiceName != "svc" || got[0].NodeType != "dbt-seed" {
		t.Fatalf("toDomain fields wrong: %+v", got)
	}
}
