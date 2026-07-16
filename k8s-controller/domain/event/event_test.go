package event

import (
	"testing"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
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

// TestTaskRetry_ToMap_CarriesRuntimeManifestRef pins the retry to the artifact
// the failed attempt ran against: without all four fields on the wire, the
// executor rebuilds the task against no runtime manifest and reroutes it off the
// pool that served it.
func TestTaskRetry_ToMap_CarriesRuntimeManifestRef(t *testing.T) {
	ref := completeRef()
	e := TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1", RuntimeManifestRef: ref}
	m := e.ToMap()

	for field, want := range map[string]string{
		"runtime_manifest_uri":                  ref.RuntimeManifestURI,
		"runtime_manifest_sha256":               ref.RuntimeManifestSHA256,
		"runtime_manifest_dbt_version":          ref.RuntimeManifestDBTVersion,
		"runtime_manifest_parse_context_sha256": ref.RuntimeManifestParseContextSHA256,
	} {
		if got, _ := m[field].(string); got != want {
			t.Errorf("ToMap()[%q]: expected %q, got %v", field, want, m[field])
		}
	}
}

// TestTaskRetry_ToMap_OmitsZeroRuntimeManifestRef keeps a task that has no
// runtime manifest wire-identical to before the field existed. A half-filled
// reference is a contract violation, so the zero reference emits no field at all
// rather than four empty ones — which the executor's parser would reject.
func TestTaskRetry_ToMap_OmitsZeroRuntimeManifestRef(t *testing.T) {
	e := TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1"}
	m := e.ToMap()

	for _, field := range []string{
		"runtime_manifest_uri",
		"runtime_manifest_sha256",
		"runtime_manifest_dbt_version",
		"runtime_manifest_parse_context_sha256",
	} {
		if _, present := m[field]; present {
			t.Errorf("ToMap()[%q]: expected absent for a task with no runtime manifest, got %v", field, m[field])
		}
	}
}

// TestTaskRetry_ToMap_OmitsExecutorDeploymentID pins a Kubernetes Job retry to
// the fresh-deployment path. On retry.task:v1 that field means "re-attempt this
// existing row in place", which the executor accepts only for a worker task
// parked at retry_pending. A Job's row is 'deployed' when it fails, so naming it
// here would make the executor reject the retry permanently and drop it.
func TestTaskRetry_ToMap_OmitsExecutorDeploymentID(t *testing.T) {
	e := TaskRetry{TaskID: "t1", ScheduleID: "s1", JobName: "job-1", RuntimeManifestRef: completeRef()}
	if _, present := e.ToMap()["executor_deployment_id"]; present {
		t.Error("ToMap(): a Job retry must not name an executor deployment to requeue in place")
	}
}

// TestTaskRetry_ToMap_OperationPresent guards the false-green retry bug: when
// the failed Job carried a non-run operation (e.g. "test"), the retry.task:v1
// wire map must carry it so the rebuilt Job stays `dbt test`, not `dbt run`.
func TestTaskRetry_ToMap_OperationPresent(t *testing.T) {
	e := TaskRetry{
		TaskID:     "t1",
		ScheduleID: "s1",
		JobName:    "job-1",
		RetryCount: 2,
		MaxRetries: 3,
		NodeType:   "dbt-model",
		Operation:  "test",
	}
	m := e.ToMap()
	if got, _ := m["operation"].(string); got != "test" {
		t.Errorf("ToMap()[\"operation\"]: expected %q, got %v", "test", m["operation"])
	}
}

// TestTaskRetry_ToMap_OperationEmpty guards the normal `dbt run` retry path:
// an empty Operation must not corrupt the wire map (the executor parser
// defaults an absent/empty operation field to run).
func TestTaskRetry_ToMap_OperationEmpty(t *testing.T) {
	e := TaskRetry{
		TaskID:     "t1",
		ScheduleID: "s1",
		JobName:    "job-1",
		RetryCount: 1,
		MaxRetries: 3,
		NodeType:   "dbt-model",
	}
	m := e.ToMap()
	if got, _ := m["operation"].(string); got != "" {
		t.Errorf("ToMap()[\"operation\"]: expected empty for run retries, got %q", got)
	}
}
