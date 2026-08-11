// executor-controller/adapters/redis/query_model_parser.go
package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseQueryModel translates a query.model:v1 XMessage into a typed
// domain event. All errors are permanent (malformed input never becomes
// valid on retry); the binding wraps with events.ErrPermanent.
func ParseQueryModel(msg goredis.XMessage) (events.QueryModel, error) {
	taskIDStr := stringField(msg.Values, "task_id")
	scheduleIDStr := stringField(msg.Values, "schedule_id")
	if taskIDStr == "" || scheduleIDStr == "" {
		return events.QueryModel{},
			fmt.Errorf("missing task_id or schedule_id (task_id=%q schedule_id=%q)",
				taskIDStr, scheduleIDStr)
	}
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return events.QueryModel{}, fmt.Errorf("invalid task_id: %w", err)
	}
	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return events.QueryModel{}, fmt.Errorf("invalid schedule_id: %w", err)
	}
	nodeType, err := pkg_model.ParseNodeType(stringField(msg.Values, "node_type"))
	if err != nil {
		return events.QueryModel{}, fmt.Errorf("invalid node_type: %w", err)
	}
	// outbox_entry_id is the orchestrator's outbox row ID, carried as
	// provenance on the executor outbox row. Absent or empty → uuid.Nil
	// (dedup relies solely on (msg.ID, stream_name) in message_processing).
	// Present-but-malformed → permanent error.
	var outboxEntryID uuid.UUID
	if s := stringField(msg.Values, "outbox_entry_id"); s != "" {
		outboxEntryID, err = uuid.Parse(s)
		if err != nil {
			return events.QueryModel{}, fmt.Errorf("invalid outbox_entry_id: %w", err)
		}
	}
	operation, err := pkg_model.ParseOperation(stringField(msg.Values, "operation"))
	if err != nil {
		return events.QueryModel{}, fmt.Errorf("invalid operation: %w", err)
	}
	return events.QueryModel{
		OutboxEntryID: outboxEntryID,
		TaskID:        taskID,
		ScheduleID:    scheduleID,
		ScheduleName:  stringField(msg.Values, "schedule_name"),
		ServiceName:   stringField(msg.Values, "service_name"),
		SchemaName:    stringField(msg.Values, "schema_name"),
		TableName:     stringField(msg.Values, "table_name"),
		JobName:       stringField(msg.Values, "job_name"),
		NodeType:      nodeType,
		ImageTag:      stringField(msg.Values, "image_tag"),
		Operation:     operation,
	}, nil
}

// stringField safely retrieves a string value from message values.
func stringField(values map[string]interface{}, key string) string {
	if val, ok := values[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}
