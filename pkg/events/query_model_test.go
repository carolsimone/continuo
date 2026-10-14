package events_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queryModelFixture is pkg/events/testdata/query_model_v1.json: a dispatch and
// the stream fields it is written as.
type queryModelFixture struct {
	Input  events.QueryModel `json:"input"`
	Fields map[string]string `json:"fields"`
}

func TestQueryModel_FieldsMatchTheGoldenFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/query_model_v1.json")
	require.NoError(t, err)
	var fixture queryModelFixture
	require.NoError(t, json.Unmarshal(raw, &fixture))

	assert.Equal(t, fixture.Fields, fixture.Input.Fields())
}

// A plain run with no secret and no budget writes neither operation,
// secret_ref nor max_retries; a set operation is written as it is.
func TestQueryModel_FieldsOmitUnsetOptionals(t *testing.T) {
	q := events.QueryModel{
		ScheduleID: "s", ScheduleName: "daily", TaskID: "t", ServiceName: "svc", SchemaName: "raw",
		TableName: "orders", JobName: "svc-raw-orders-s", NodeType: "dbt-model", ImageTag: "svc:1",
	}
	assert.Equal(t, map[string]string{
		"schedule_id": "s", "schedule_name": "daily", "task_id": "t", "service_name": "svc", "schema_name": "raw",
		"table_name": "orders", "job_name": "svc-raw-orders-s", "node_type": "dbt-model", "image_tag": "svc:1",
	}, q.Fields())

	q.Operation = "test"
	assert.Equal(t, "test", q.Fields()["operation"])
	_, hasRetries := q.Fields()["max_retries"]
	assert.False(t, hasRetries)
}
