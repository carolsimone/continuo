package serialization

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// goldenAttemptFacts is the exact JSON every fact of an attempt is stored as
// in deployments.facts. failure_class is derived from failure_reason.
const goldenAttemptFacts = `{"started_at":"2026-10-20T23:01:24.105Z","finished_at":"2026-10-20T23:01:51.93Z","exit_code":137,"failed_container":"dbt-job","termination_reason":"OOMKilled","pod_reason":"Evicted","job_succeeded":true,"job_failure_reason":"DeadlineExceeded","message":"OOMKilled: out of memory","nothing_to_do":true,"log_s3_key":"logs/task-executions/svc/s/t/e.log","run_results_uri":"run-results/task-executions/svc/s/t/e.json","log_message":"Killed","parse_cache":"hydrated","parse_cache_reason":"cache miss","awaiting_stop_since":"2026-10-20T23:01:52Z","failure_class":"resource","failure_reason":"oom_killed"}`

func TestAttemptFactsDTORoundTrip(t *testing.T) {
	started := time.Date(2026, 10, 20, 23, 1, 24, 105_000_000, time.UTC)
	finished := time.Date(2026, 10, 20, 23, 1, 51, 930_000_000, time.UTC)
	stuck := time.Date(2026, 10, 20, 23, 1, 52, 0, time.UTC)
	code := int32(137)
	want := model.AttemptFacts{
		StartedAt: &started, FinishedAt: &finished, ExitCode: &code, FailedContainer: "dbt-job",
		TerminationReason: "OOMKilled", PodReason: "Evicted", JobSucceeded: true, JobFailureReason: "DeadlineExceeded",
		Message: "OOMKilled: out of memory", NothingToDo: true,
		LogKey: "logs/task-executions/svc/s/t/e.log", RunResultsKey: "run-results/task-executions/svc/s/t/e.json",
		LogMessage: "Killed", ParseCache: "hydrated", ParseCacheReason: "cache miss",
		AwaitingStopSince: &stuck, Reason: pkgmodel.FailureReasonOOMKilled,
	}

	out, err := json.Marshal(AttemptFactsFromDomain(want))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenAttemptFacts {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenAttemptFacts)
	}
	var dto AttemptFactsDTO
	if err := json.Unmarshal([]byte(goldenAttemptFacts), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	if got := dto.ToDomain(); !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
}

func TestAttemptFactsDTO_NoFactsEncodesEmpty(t *testing.T) {
	out, err := json.Marshal(AttemptFactsFromDomain(model.AttemptFacts{}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != `{}` {
		t.Fatalf("empty facts = %s, want {}", out)
	}
}
