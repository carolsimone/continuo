package serialization

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
)

// goldenDeployTask is the exact JSON a fully-populated command.DeployTask is
// stored as in executor_deployments.job_params. Every tagged field is present.
const goldenDeployTask = `{"task_id":"t","schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","job_name":"j","node_type":"dbt-model","image_tag":"v1","task_retry_count":1,"task_max_retries":3,"operation":"test","mode":"promote_seed"}`

func TestDeployTaskDTORoundTrip(t *testing.T) {
	var dto DeployTaskDTO
	if err := json.Unmarshal([]byte(goldenDeployTask), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := command.DeployTask{
		TaskID: "t", ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc",
		SchemaName: "sch", TableName: "tbl", JobName: "j", NodeType: "dbt-model",
		ImageTag: "v1", TaskRetryCount: 1, TaskMaxRetries: 3, Operation: "test", Mode: "promote_seed",
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

func TestDeployTaskModeOmitempty(t *testing.T) {
	out, err := json.Marshal(DeployTaskFromDomain(command.DeployTask{TaskID: "t"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"task_id":"t","schedule_id":"","schedule_name":"","service_name":"","schema_name":"","table_name":"","job_name":"","node_type":"","image_tag":"","task_retry_count":0,"task_max_retries":0,"operation":""}`
	if string(out) != want {
		t.Fatalf("mode omitempty not preserved:\n got %s\nwant %s", out, want)
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
