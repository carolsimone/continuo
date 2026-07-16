package redis

import (
	"encoding/json"
	"testing"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func msgWith(values map[string]interface{}) goredis.XMessage {
	return goredis.XMessage{ID: "1-0", Values: values}
}

// payloadMsg wraps a typed event as the JSON `payload` field of a Redis message,
// matching the wire shape the producers emit.
func payloadMsg(t *testing.T, evt interface{}) goredis.XMessage {
	t.Helper()
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return msgWith(map[string]interface{}{"payload": string(b)})
}

func TestParseNodeDeployed_DecodesPayloadAndTaskRetryCount(t *testing.T) {
	taskID := uuid.New()
	schedID := uuid.New()
	cmd, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:         taskID.String(),
		ScheduleID:     schedID.String(),
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "public",
		TableName:      "orders",
		JobName:        "job-1",
		NodeType:       "model",
		ImageTag:       "sha-abc",
		TaskRetryCount: 2,
		MaxRetries:     5,
	}), 3)
	if err != nil {
		t.Fatalf("ParseNodeDeployed: %v", err)
	}
	if cmd.TaskID != taskID || cmd.ScheduleID != schedID {
		t.Fatalf("ids not mapped: %+v", cmd)
	}
	if cmd.JobName != "job-1" || cmd.ImageTag != "sha-abc" || cmd.NodeType != "model" {
		t.Fatalf("string fields not mapped: %+v", cmd)
	}
	if cmd.RetryCount != 2 {
		t.Fatalf("expected retry_count from task_retry_count=2, got %d", cmd.RetryCount)
	}
	if cmd.MaxRetries != 5 {
		t.Fatalf("expected max_retries 5, got %d", cmd.MaxRetries)
	}
	if cmd.ScheduleName != "daily" || cmd.ServiceName != "svc" || cmd.SchemaName != "public" || cmd.TableName != "orders" {
		t.Fatalf("name/service/schema/table fields not mapped: %+v", cmd)
	}
}

func TestParseCheckK8s_DecodesPayloadAndRetryCount(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-2",
		RetryCount: 4,
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.RetryCount != 4 {
		t.Fatalf("expected retry_count 4, got %d", cmd.RetryCount)
	}
}

// TestParseCheckK8s_CarriesRunningAnnounced verifies the re-poll loop preserves the
// per-attempt "already announced RUNNING" flag across check.k8s:v1 hops.
func TestParseCheckK8s_CarriesRunningAnnounced(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:           uuid.New().String(),
		ScheduleID:       uuid.New().String(),
		JobName:          "job-ra",
		RunningAnnounced: true,
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if !cmd.RunningAnnounced {
		t.Fatal("expected RunningAnnounced=true carried from check.k8s:v1 payload")
	}
}

// TestParseNodeDeployed_RunningAnnouncedDefaultsFalse verifies a fresh attempt
// (the node.deployed:v1 that starts an attempt) has not yet announced RUNNING.
func TestParseNodeDeployed_RunningAnnouncedDefaultsFalse(t *testing.T) {
	cmd, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-fresh",
	}), 3)
	if err != nil {
		t.Fatalf("ParseNodeDeployed: %v", err)
	}
	if cmd.RunningAnnounced {
		t.Fatal("expected RunningAnnounced=false for a fresh node.deployed attempt")
	}
}

func TestParseCheckK8s_DefaultMaxRetriesWhenAbsent(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-3",
	}), 7)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.MaxRetries != 7 {
		t.Fatalf("expected default max_retries 7, got %d", cmd.MaxRetries)
	}
}

func TestParseNodeDeployed_InvalidTaskIDErrors(t *testing.T) {
	_, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:     "not-a-uuid",
		ScheduleID: uuid.New().String(),
	}), 3)
	if err == nil {
		t.Fatal("expected error for invalid task_id")
	}
}

func TestParseNodeDeployed_MissingPayloadErrors(t *testing.T) {
	_, err := ParseNodeDeployed(msgWith(map[string]interface{}{}), 3)
	if err == nil {
		t.Fatal("expected error when payload field is absent")
	}
}

func TestParseCheckK8s_InvalidJSONErrors(t *testing.T) {
	_, err := ParseCheckK8s(msgWith(map[string]interface{}{"payload": "{not json"}), 3)
	if err == nil {
		t.Fatal("expected error for malformed payload JSON")
	}
}

// TestParseNodeDeployed_CarriesOperation verifies the dbt verb rides the durable
// node.deployed:v1 payload into CheckJobStatus, so it never depends on Job labels.
func TestParseNodeDeployed_CarriesOperation(t *testing.T) {
	cmd, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-op",
		Operation:  "test",
	}), 3)
	if err != nil {
		t.Fatalf("ParseNodeDeployed: %v", err)
	}
	if cmd.Operation != "test" {
		t.Fatalf("expected Operation=test from node.deployed payload, got %q", cmd.Operation)
	}
}

// TestParseCheckK8s_CarriesOperation verifies the re-poll loop preserves the dbt
// verb across check.k8s:v1 hops, so a check that lands after the Job is TTL-reaped
// still retains the verb for retry.
func TestParseCheckK8s_CarriesOperation(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-op",
		Operation:  "test",
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.Operation != "test" {
		t.Fatalf("expected Operation=test carried from check.k8s payload, got %q", cmd.Operation)
	}
}

// parserRef is a complete runtime manifest reference used to assert the pin
// survives both legs of the durable check chain.
func parserRef() pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://artifacts/svc/manifest.msgpack",
		RuntimeManifestSHA256:             "9f2c1b4e7a6d5038c9b1e2f4a7d6c5b8093e1f2a4b6c8d0e2f4a6b8c0d2e4f60",
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f809",
	}
}

// TestParseNodeDeployed_CarriesCapacityAndRuntimeFields covers the first leg of
// the durable chain: the deployment id that owns the Job's execution slot, the
// dispatch mode that routes its terminal result, and the artifact pin all enter
// the command from the message rather than from Job metadata.
func TestParseNodeDeployed_CarriesCapacityAndRuntimeFields(t *testing.T) {
	depID := uuid.New()
	cmd, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:               uuid.New().String(),
		ScheduleID:           uuid.New().String(),
		JobName:              "job-1",
		ExecutorDeploymentID: depID.String(),
		Mode:                 pkgevents.ModePromoteSeed,
		RuntimeManifestRef:   parserRef(),
	}), 3)
	if err != nil {
		t.Fatalf("ParseNodeDeployed: %v", err)
	}
	if cmd.ExecutorDeploymentID != depID.String() {
		t.Errorf("ExecutorDeploymentID: expected %q, got %q", depID, cmd.ExecutorDeploymentID)
	}
	if cmd.Mode != pkgevents.ModePromoteSeed {
		t.Errorf("Mode: expected %q, got %q", pkgevents.ModePromoteSeed, cmd.Mode)
	}
	if cmd.RuntimeManifestRef != parserRef() {
		t.Errorf("RuntimeManifestRef: expected %+v, got %+v", parserRef(), cmd.RuntimeManifestRef)
	}
}

// TestParseCheckK8s_CarriesCapacityAndRuntimeFields covers the self-poll leg:
// the same three values must recirculate on every check, or a terminal observed
// after the Job is TTL-reaped could neither release its slot nor route correctly.
func TestParseCheckK8s_CarriesCapacityAndRuntimeFields(t *testing.T) {
	depID := uuid.New()
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:               uuid.New().String(),
		ScheduleID:           uuid.New().String(),
		JobName:              "job-1",
		ExecutorDeploymentID: depID.String(),
		Mode:                 "validation",
		RuntimeManifestRef:   parserRef(),
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.ExecutorDeploymentID != depID.String() {
		t.Errorf("ExecutorDeploymentID: expected %q, got %q", depID, cmd.ExecutorDeploymentID)
	}
	if cmd.Mode != "validation" {
		t.Errorf("Mode: expected %q, got %q", "validation", cmd.Mode)
	}
	if cmd.RuntimeManifestRef != parserRef() {
		t.Errorf("RuntimeManifestRef: expected %+v, got %+v", parserRef(), cmd.RuntimeManifestRef)
	}
}

// TestParseNodeDeployed_CapacityFieldsAbsentStayEmpty keeps a Job dispatched
// before these fields existed parseable: absent is valid and yields empty, which
// the terminal check reads as "no slot to release".
func TestParseNodeDeployed_CapacityFieldsAbsentStayEmpty(t *testing.T) {
	cmd, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-1",
	}), 3)
	if err != nil {
		t.Fatalf("ParseNodeDeployed: %v", err)
	}
	if cmd.ExecutorDeploymentID != "" || cmd.Mode != "" {
		t.Errorf("expected empty capacity fields, got id=%q mode=%q", cmd.ExecutorDeploymentID, cmd.Mode)
	}
	if cmd.RuntimeManifestRef != (pkgmodel.RuntimeManifestRef{}) {
		t.Errorf("expected zero runtime ref, got %+v", cmd.RuntimeManifestRef)
	}
}
