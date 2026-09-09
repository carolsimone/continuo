package serialization

import (
	"encoding/json"
	"testing"
)

const goldenReleaseSeedsPending = `{"release_id":"r","nodes":[{"service_name":"svc","schema_name":"sch","table_name":"tbl","node_type":"dbt-seed","image_tag":"img"}]}`

func TestReleaseSeedsPendingRoundTrip(t *testing.T) {
	var dto ReleaseSeedsPendingDTO
	if err := json.Unmarshal([]byte(goldenReleaseSeedsPending), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	// Byte shape: re-marshalling the decoded DTO reproduces the payload.
	out, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenReleaseSeedsPending {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenReleaseSeedsPending)
	}
	got := dto.ToDomain()
	if got.ReleaseID != "r" || len(got.Nodes) != 1 {
		t.Fatalf("toDomain: %+v", got)
	}
	n := got.Nodes[0]
	if n.ServiceName != "svc" || n.SchemaName != "sch" || n.TableName != "tbl" || n.NodeType != "dbt-seed" || n.ImageTag != "img" {
		t.Fatalf("toDomain node fields wrong: %+v", n)
	}
}
