package events_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/events"
)

var taskStatusReportFixtures = []string{
	"testdata/task_status_updated_v2_queued.json",
	"testdata/task_status_updated_v2_running.json",
	"testdata/task_status_updated_v2_succeeded.json",
	"testdata/task_status_updated_v2_failed.json",
}

type taskStatusReportFixture struct {
	Input struct {
		EventID    string                  `json:"event_id"`
		TenantID   string                  `json:"tenant_id"`
		Producer   string                  `json:"producer"`
		OccurredAt time.Time               `json:"occurred_at"`
		Payload    events.TaskStatusReport `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func loadTaskStatusReportFixture(t *testing.T, path string) taskStatusReportFixture {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is one of the fixed fixture paths above
	require.NoError(t, err)
	var fx taskStatusReportFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func TestTaskStatusReportFields_MatchGoldenFixtures(t *testing.T) {
	for _, path := range taskStatusReportFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadTaskStatusReportFixture(t, path)
			got, err := events.TaskStatusReportFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
			require.NoError(t, err)
			assertFieldsMatchFixture(t, fx.Fields, got)
		})
	}
}

func TestDecodeTaskStatusReport_ReadsGoldenFixtures(t *testing.T) {
	for _, path := range taskStatusReportFixtures {
		t.Run(path, func(t *testing.T) {
			fx := loadTaskStatusReportFixture(t, path)
			env, p, err := events.DecodeTaskStatusReport(fixtureFields(t, fx.Fields))
			require.NoError(t, err)
			assert.Equal(t, fx.Input.Payload, p)
			assert.Equal(t, fx.Input.EventID, env.EventID)
			assert.Equal(t, fx.Input.Producer, env.Producer)
		})
	}
}

func TestTaskStatusReportFields_WritesTimesInUTC(t *testing.T) {
	cest := time.FixedZone("CEST", 2*60*60)
	at := time.Date(2026, 10, 21, 1, 1, 20, 0, cest)
	got, err := events.TaskStatusReportFields("e", "default", "execution-controller", at, events.TaskStatusReport{
		RunID: "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b", TaskID: "3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a",
		Status: events.AttemptQueued, QueuedAt: &at,
	})
	require.NoError(t, err)
	assert.Contains(t, got["payload"], `"queued_at":"2026-10-20T23:01:20Z"`)
}

// A report that cannot say which attempt it describes, or how a failed one
// failed, can never be applied.
func TestDecodeTaskStatusReport_RejectsUnreadableReports(t *testing.T) {
	const ids = `"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a"`
	cases := map[string]map[string]string{
		"no envelope":              {"payload": `{` + ids + `,"attempt":0,"status":"running"}`},
		"other schema version":     withField(reportFields(`"attempt":0,"status":"running"`), "schema_version", "2"),
		"payload not json":         withPayload(reportFields(`"attempt":0,"status":"running"`), `{`),
		"run_id not a uuid":        withPayload(reportFields(``), `{"run_id":"r1","task_id":"3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a","attempt":0,"status":"running"}`),
		"task_id not a uuid":       withPayload(reportFields(``), `{"run_id":"7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b","task_id":"t1","attempt":0,"status":"running"}`),
		"no attempt":               reportFields(`"status":"running"`),
		"attempt not integer":      reportFields(`"attempt":"0","status":"running"`),
		"negative attempt":         reportFields(`"attempt":-1,"status":"running"`),
		"unknown status":           reportFields(`"attempt":0,"status":"dispatched"`),
		"failed without reason":    reportFields(`"attempt":0,"status":"failed"`),
		"failed without class":     reportFields(`"attempt":0,"status":"failed","failure_reason":"oom_killed"`),
		"undeclared reason":        reportFields(`"attempt":0,"status":"failed","failure_class":"task","failure_reason":"gremlins"`),
		"class of another reason":  reportFields(`"attempt":0,"status":"failed","failure_class":"task","failure_reason":"oom_killed"`),
		"queued naming a failure":  reportFields(`"attempt":0,"status":"queued","failure_class":"task","failure_reason":"exit_code"`),
		"running naming a failure": reportFields(`"attempt":0,"status":"running","failure_reason":"exit_code"`),
		"success with a failure":   reportFields(`"attempt":0,"status":"succeeded","failure_class":"task","failure_reason":"exit_code"`),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeTaskStatusReport(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

// A failed report whose class is its reason's class is readable.
func TestDecodeTaskStatusReport_ReadsAFailureWhoseClassIsTheReasons(t *testing.T) {
	_, p, err := events.DecodeTaskStatusReport(reportFields(`"attempt":3,"status":"failed","failure_class":"resource","failure_reason":"oom_killed"`))
	require.NoError(t, err)
	assert.Equal(t, int32(3), p.Attempt)
	assert.Equal(t, "oom_killed", p.FailureReason)
}

// reportFields is a task.status.updated:v2 entry of execution-controller for
// the run and task of validDispatchFields, with extra payload members.
func reportFields(extra string) map[string]string {
	fields := validDispatchFields(extra)
	fields["producer"] = "execution-controller"
	return fields
}
