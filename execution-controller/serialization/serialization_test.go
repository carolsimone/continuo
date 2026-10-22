package serialization

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
)

// goldenDeployTask is the exact JSON a fully-populated command.DeployTask is
// stored as in deployments.job_params. Every tagged field is present.
const goldenDeployTask = `{"task_id":"t","schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","job_name":"j","node_type":"dbt-model","image_tag":"v1","attempt":1,"operation":"test","timeout_seconds":86400}`

func TestDeployTaskDTORoundTrip(t *testing.T) {
	var dto DeployTaskDTO
	if err := json.Unmarshal([]byte(goldenDeployTask), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := command.DeployTask{
		TaskID: "t", ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc",
		SchemaName: "sch", TableName: "tbl", JobName: "j", NodeType: "dbt-model",
		ImageTag: "v1", Attempt: 1, Operation: "test", TimeoutSeconds: 86400,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(DeployTaskFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenDeployTask {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenDeployTask)
	}
}

// A row queued by an older version may still hold a mode in its job_params;
// it decodes, and the mode is dropped.
func TestDeployTaskDTO_StoredModeIsIgnored(t *testing.T) {
	var dto DeployTaskDTO
	if err := json.Unmarshal([]byte(`{"task_id":"t","job_name":"j","mode":"promote_seed"}`), &dto); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), "mode") {
		t.Fatalf("the mode survived: %s", out)
	}
}

// goldenValidationDeployTask is the exact JSON a fully-populated
// command.ValidationDeployTask is stored as.
const goldenValidationDeployTask = `{"release_id":"r","node_id":"n","service_name":"svc","schema_name":"sch","table_name":"tbl","node_type":"dbt-model","image_tag":"v1","job_name":"j","candidate_schema":"cand","candidate_artifact_uri":"s3://c","validation_op":"build_from_sql","prod_schema":"prod","upstream_node_ids":["u1"],"manifest_s3_uri":"s3://m","parse_prod_s3_uri":"s3://pp","parse_candidate_s3_uri":"s3://pc","source_overlay_uri":"s3://so"}`

func TestValidationDeployTaskDTORoundTrip(t *testing.T) {
	var dto ValidationDeployTaskDTO
	if err := json.Unmarshal([]byte(goldenValidationDeployTask), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := command.ValidationDeployTask{
		ReleaseID: "r", NodeID: "n", ServiceName: "svc", SchemaName: "sch", TableName: "tbl",
		NodeType: "dbt-model", ImageTag: "v1", JobName: "j", CandidateSchema: "cand",
		CandidateArtifactURI: "s3://c", ValidationOp: "build_from_sql", ProdSchema: "prod",
		UpstreamNodeIDs: []string{"u1"}, ManifestS3URI: "s3://m",
		ParseProdS3URI: "s3://pp", ParseCandidateS3URI: "s3://pc", SourceOverlayURI: "s3://so",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(ValidationDeployTaskFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenValidationDeployTask {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenValidationDeployTask)
	}
}

func TestValidationDeployTaskOmitemptyShape(t *testing.T) {
	out, err := json.Marshal(ValidationDeployTaskFromDomain(command.ValidationDeployTask{ReleaseID: "r"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// parse_prod_s3_uri / parse_candidate_s3_uri / source_overlay_uri are omitempty
	// and absent; upstream_node_ids has no omitempty so a nil slice is null.
	const want = `{"release_id":"r","node_id":"","service_name":"","schema_name":"","table_name":"","node_type":"","image_tag":"","job_name":"","candidate_schema":"","candidate_artifact_uri":"","validation_op":"","prod_schema":"","upstream_node_ids":null,"manifest_s3_uri":""}`
	if string(out) != want {
		t.Fatalf("omitempty shape changed:\n got %s\nwant %s", out, want)
	}
}

func TestDeployTaskDTO_SecretRefRoundTripsAndIsOmittedWhenEmpty(t *testing.T) {
	cmd := command.DeployTask{TaskID: "t", SecretRef: "continuo-api-fx"}
	raw, err := json.Marshal(DeployTaskFromDomain(cmd))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"secret_ref":"continuo-api-fx"`) {
		t.Fatalf("secret_ref missing from %s", raw)
	}
	var back DeployTaskDTO
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := back.ToDomain().SecretRef; got != "continuo-api-fx" {
		t.Fatalf("SecretRef = %q after round trip", got)
	}

	raw, err = json.Marshal(DeployTaskFromDomain(command.DeployTask{TaskID: "t"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "secret_ref") {
		t.Fatalf("empty secret_ref must be omitted, got %s", raw)
	}
}

// TestDeployTaskDTO_StoredJobParamsWithoutSecretRefDecodeEmpty covers rows
// persisted before a node carried a secret_ref: they decode to an empty ref.
func TestDeployTaskDTO_StoredJobParamsWithoutSecretRefDecodeEmpty(t *testing.T) {
	var back DeployTaskDTO
	if err := json.Unmarshal([]byte(goldenDeployTask), &back); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	if got := back.ToDomain().SecretRef; got != "" {
		t.Fatalf("SecretRef = %q, want empty", got)
	}
}
