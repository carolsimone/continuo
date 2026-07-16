// executor-controller/adapters/redis/query_model_parser_test.go
package redis

import (
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQueryModel_HappyPath(t *testing.T) {
	taskID := uuid.New()
	scheduleID := uuid.New()
	outboxEntryID := uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"outbox_entry_id": outboxEntryID.String(),
		"task_id":         taskID.String(),
		"schedule_id":     scheduleID.String(),
		"schedule_name":   "daily",
		"service_name":    "dbt",
		"schema_name":     "public",
		"table_name":      "orders",
		"job_name":        "dbt-public-orders",
		"node_type":       "dbt-model",
		"image_tag":       "sha-abc",
	}}
	evt, err := ParseQueryModel(msg)
	require.NoError(t, err)
	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, taskID, evt.TaskID)
	assert.Equal(t, scheduleID, evt.ScheduleID)
	assert.Equal(t, "daily", evt.ScheduleName)
	assert.Equal(t, "dbt", evt.ServiceName)
	assert.Equal(t, "public", evt.SchemaName)
	assert.Equal(t, "orders", evt.TableName)
	assert.Equal(t, "dbt-public-orders", evt.JobName)
	assert.Equal(t, pkg_model.NodeTypeDbtModel, evt.NodeType)
	assert.Equal(t, "sha-abc", evt.ImageTag)
}

func TestParseQueryModel_OutboxEntryIDAbsentIsNilUUID(t *testing.T) {
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID,
		"absent outbox_entry_id is uuid.Nil so dedup degrades to (msg.ID, stream_name)")
}

func TestParseQueryModel_OutboxEntryIDInvalidIsPermanentError(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"outbox_entry_id": "not-a-uuid",
		"task_id":         uuid.New().String(),
		"schedule_id":     uuid.New().String(),
		"node_type":       "dbt-model",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_MissingTaskID(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_InvalidScheduleID(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": "not-a-uuid",
		"node_type":   "dbt-model",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_UnknownNodeType(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "no_such_type",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_OperationTestParses(t *testing.T) {
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
		"operation":   "test",
	}})
	require.NoError(t, err)
	assert.Equal(t, pkg_model.OperationTest, evt.Operation)
}

func TestParseQueryModel_OperationAbsentDefaultsToRun(t *testing.T) {
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.NoError(t, err)
	assert.Equal(t, pkg_model.OperationRun, evt.Operation)
}

func TestParseQueryModel_InvalidOperationIsPermanentError(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
		"operation":   "no_such_operation",
	}})
	require.Error(t, err)
}

// runtimeManifestFields is a complete, well-formed pin as it travels on the wire.
func runtimeManifestFields() map[string]interface{} {
	return map[string]interface{}{
		"runtime_manifest_uri":                  "s3://artifacts/finance/partial_parse.msgpack",
		"runtime_manifest_sha256":               "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
		"runtime_manifest_dbt_version":          "1.12.0b1",
		"runtime_manifest_parse_context_sha256": "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
	}
}

func queryModelMsg(extra map[string]interface{}) goredis.XMessage {
	values := map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}
	for k, v := range extra {
		values[k] = v
	}
	return goredis.XMessage{ID: "1-0", Values: values}
}

func TestParseQueryModel_RuntimeManifestPinParses(t *testing.T) {
	fields := runtimeManifestFields()
	fields["dbt_unique_id"] = "model.finance.orders"

	evt, err := ParseQueryModel(queryModelMsg(fields))
	require.NoError(t, err)
	assert.Equal(t, "model.finance.orders", evt.DBTUniqueID)
	assert.Equal(t, fields["runtime_manifest_uri"], evt.RuntimeManifestURI)
	assert.Equal(t, fields["runtime_manifest_sha256"], evt.RuntimeManifestSHA256)
	assert.Equal(t, fields["runtime_manifest_dbt_version"], evt.RuntimeManifestDBTVersion)
	assert.Equal(t, fields["runtime_manifest_parse_context_sha256"], evt.RuntimeManifestParseContextSHA256)
	assert.True(t, evt.Complete())
}

// TestParseQueryModel_HistoricalMessageCarriesNoPin pins backward compatibility:
// a message produced before nodes carried their dbt identity parses cleanly and
// yields the zero reference, which routes it to the Kubernetes Job path.
func TestParseQueryModel_HistoricalMessageCarriesNoPin(t *testing.T) {
	evt, err := ParseQueryModel(queryModelMsg(nil))
	require.NoError(t, err)
	assert.Empty(t, evt.DBTUniqueID)
	assert.Equal(t, pkg_model.RuntimeManifestRef{}, evt.RuntimeManifestRef)
	assert.NoError(t, evt.Validate())
}

// TestParseQueryModel_PartialRuntimeManifestIsPermanentError pins the
// all-or-none contract: a half-filled pin cannot be fetched, verified or
// reused, so it is rejected rather than carried as an unusable reference.
func TestParseQueryModel_PartialRuntimeManifestIsPermanentError(t *testing.T) {
	for _, dropped := range []string{
		"runtime_manifest_uri",
		"runtime_manifest_sha256",
		"runtime_manifest_dbt_version",
		"runtime_manifest_parse_context_sha256",
	} {
		t.Run("without_"+dropped, func(t *testing.T) {
			fields := runtimeManifestFields()
			delete(fields, dropped)
			fields["dbt_unique_id"] = "model.finance.orders"

			_, err := ParseQueryModel(queryModelMsg(fields))
			require.Error(t, err)
		})
	}
}

func TestParseQueryModel_MalformedRuntimeManifestIsPermanentError(t *testing.T) {
	cases := map[string]map[string]interface{}{
		"non_s3_uri":       {"runtime_manifest_uri": "https://artifacts/partial_parse.msgpack"},
		"truncated_digest": {"runtime_manifest_sha256": "abc123"},
		"uppercase_digest": {"runtime_manifest_parse_context_sha256": "0F1E2D3C4B5A69788796A5B4C3D2E1F00F1E2D3C4B5A69788796A5B4C3D2E1F0"},
	}
	for name, overrides := range cases {
		t.Run(name, func(t *testing.T) {
			fields := runtimeManifestFields()
			for k, v := range overrides {
				fields[k] = v
			}
			fields["dbt_unique_id"] = "model.finance.orders"

			_, err := ParseQueryModel(queryModelMsg(fields))
			require.Error(t, err)
		})
	}
}

// TestParseQueryModel_DBTUniqueIDWithoutAPinParses pins that the two carriers
// are independent on the wire: a node can be migrated (it has a dbt identity)
// while its release published no runtime manifest. Routing, not parsing, decides
// whether that combination can run.
func TestParseQueryModel_DBTUniqueIDWithoutAPinParses(t *testing.T) {
	evt, err := ParseQueryModel(queryModelMsg(map[string]interface{}{
		"dbt_unique_id": "model.finance.orders",
	}))
	require.NoError(t, err)
	assert.Equal(t, "model.finance.orders", evt.DBTUniqueID)
	assert.False(t, evt.Complete())
}
