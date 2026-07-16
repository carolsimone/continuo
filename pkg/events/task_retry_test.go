package events_test

import (
	"testing"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
)

// completeRef is a well-formed runtime manifest reference: all four fields set,
// s3:// URI, lowercase 64-char hex digests — the shape RuntimeManifestRef.Validate
// accepts, and therefore the shape the executor's retry parser accepts.
func completeRef() pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://artifacts/finance/manifest.msgpack",
		RuntimeManifestSHA256:             "9f2c1b4e7a6d5038c9b1e2f4a7d6c5b8093e1f2a4b6c8d0e2f4a6b8c0d2e4f60",
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f809",
	}
}

// TestTaskRetry_ToMap_UsesTaskRetryCountKey pins the count's wire name. The
// executor's retry parser reads task_retry_count; emitting retry_count would make
// every retry restart at attempt zero and loop until the Job budget ran out.
func TestTaskRetry_ToMap_UsesTaskRetryCountKey(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1", RetryCount: 2, MaxRetries: 3}.ToMap()

	assert.Equal(t, 2, m["task_retry_count"], "the executor parser keys on task_retry_count")
	assert.NotContains(t, m, "retry_count")
	assert.Equal(t, 3, m["max_retries"])
}

// TestTaskRetry_ToMap_CarriesRuntimeManifestRef pins the retry to the artifact
// the failed attempt ran against: without all four fields on the wire, the
// executor rebuilds the task against no runtime manifest and reroutes it off the
// pool that served it.
func TestTaskRetry_ToMap_CarriesRuntimeManifestRef(t *testing.T) {
	ref := completeRef()
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1", RuntimeManifestRef: ref}.ToMap()

	assert.Equal(t, ref.RuntimeManifestURI, m["runtime_manifest_uri"])
	assert.Equal(t, ref.RuntimeManifestSHA256, m["runtime_manifest_sha256"])
	assert.Equal(t, ref.RuntimeManifestDBTVersion, m["runtime_manifest_dbt_version"])
	assert.Equal(t, ref.RuntimeManifestParseContextSHA256, m["runtime_manifest_parse_context_sha256"])
}

// TestTaskRetry_ToMap_OmitsZeroRuntimeManifestRef keeps a task that has no
// runtime manifest wire-identical to before the field existed. A half-filled
// reference is a contract violation, so the zero reference emits no field at all
// rather than four empty ones — which the executor's parser would reject.
func TestTaskRetry_ToMap_OmitsZeroRuntimeManifestRef(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1"}.ToMap()

	for _, field := range []string{
		"runtime_manifest_uri",
		"runtime_manifest_sha256",
		"runtime_manifest_dbt_version",
		"runtime_manifest_parse_context_sha256",
	} {
		assert.NotContains(t, m, field, "a task with no runtime manifest emits no partial reference")
	}
}

// TestTaskRetry_ToMap_OmitsExecutorDeploymentID pins a Kubernetes Job retry to
// the fresh-deployment path. On retry.task:v1 that field means "re-attempt this
// existing row in place", which the executor accepts only for a worker task
// parked at retry_pending. A Job's row is 'deployed' when it fails, so naming it
// here would make the executor reject the retry permanently and drop it.
func TestTaskRetry_ToMap_OmitsExecutorDeploymentID(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1", RuntimeManifestRef: completeRef()}.ToMap()

	assert.NotContains(t, m, "executor_deployment_id",
		"a Job retry must not name an executor deployment to requeue in place")
}

// TestTaskRetry_ToMap_CarriesExecutorDeploymentID pins the worker retry's
// in-place requeue. The executor re-attempts the named row so the task's lease
// history and attempt counter stay on one row; without the field the retry would
// enqueue a second row competing for the same task.
func TestTaskRetry_ToMap_CarriesExecutorDeploymentID(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1",
		ExecutorDeploymentID: "3f2b1c4d-5e6f-4081-92a3-b4c5d6e7f809"}.ToMap()

	assert.Equal(t, "3f2b1c4d-5e6f-4081-92a3-b4c5d6e7f809", m["executor_deployment_id"])
}

// TestTaskRetry_ToMap_CarriesDBTUniqueID keeps the retried task bound to the one
// dbt node it invokes, so a re-attempt selects the same node rather than
// resolving it again from the table name.
func TestTaskRetry_ToMap_CarriesDBTUniqueID(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1",
		DBTUniqueID: "model.finance.orders"}.ToMap()

	assert.Equal(t, "model.finance.orders", m["dbt_unique_id"])
}

func TestTaskRetry_ToMap_OmitsEmptyDBTUniqueID(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1"}.ToMap()

	assert.NotContains(t, m, "dbt_unique_id")
}

// TestTaskRetry_ToMap_OperationPresent guards the false-green retry bug: when
// the failed attempt carried a non-run operation (e.g. "test"), the retry.task:v1
// wire map must carry it so the rebuilt task stays `dbt test`, not `dbt run`.
func TestTaskRetry_ToMap_OperationPresent(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1",
		RetryCount: 2, MaxRetries: 3, NodeType: "dbt-model", Operation: "test"}.ToMap()

	assert.Equal(t, "test", m["operation"])
}

// TestTaskRetry_ToMap_OperationEmpty guards the normal `dbt run` retry path: an
// empty Operation must not corrupt the wire map (the executor parser defaults an
// absent/empty operation field to run).
func TestTaskRetry_ToMap_OperationEmpty(t *testing.T) {
	m := events.TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1",
		RetryCount: 1, MaxRetries: 3, NodeType: "dbt-model"}.ToMap()

	assert.NotContains(t, m, "operation")
}

// TestTaskRetry_ToMap_CarriesIdentity pins the fields the executor rebuilds the
// task from.
func TestTaskRetry_ToMap_CarriesIdentity(t *testing.T) {
	m := events.TaskRetry{
		TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "finance",
		SchemaName: "public", TableName: "orders", JobName: "job-1", ImageTag: "sha-abc",
		NodeType: "dbt-model",
	}.ToMap()

	assert.Equal(t, "t1", m["task_id"])
	assert.Equal(t, "s1", m["schedule_id"])
	assert.Equal(t, "daily", m["schedule_name"])
	assert.Equal(t, "finance", m["service_name"])
	assert.Equal(t, "public", m["schema_name"])
	assert.Equal(t, "orders", m["table_name"])
	assert.Equal(t, "job-1", m["job_name"])
	assert.Equal(t, "sha-abc", m["image_tag"])
	assert.Equal(t, "dbt-model", m["node_type"])
}
