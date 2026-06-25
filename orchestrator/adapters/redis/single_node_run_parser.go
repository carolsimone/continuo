package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseSingleNodeRun translates a trigger.single_node_run:v1 XMessage into a
// SingleNodeRunInput. The node-identifying fields are required, non-empty
// strings. The metadata_source/source_run_id pair carries a cross-field rule:
// "latest" forbids a source_run_id, "snapshot_of_run" requires one, and any
// other value is rejected. Every violation is an events.ErrPermanent-wrapped
// error so the consumer ACKs the poison message instead of retrying it.
func ParseSingleNodeRun(msg goredis.XMessage) (model.SingleNodeRunInput, error) {
	scheduleID, err := requireUUIDField(msg, "schedule_id")
	if err != nil {
		return model.SingleNodeRunInput{}, err
	}
	scheduleName, err := requireString(msg, "schedule_name")
	if err != nil {
		return model.SingleNodeRunInput{}, err
	}
	schemaName, err := requireString(msg, "schema_name")
	if err != nil {
		return model.SingleNodeRunInput{}, err
	}
	tableName, err := requireString(msg, "table_name")
	if err != nil {
		return model.SingleNodeRunInput{}, err
	}
	serviceName, err := requireString(msg, "service_name")
	if err != nil {
		return model.SingleNodeRunInput{}, err
	}

	metadataSource, _ := msg.Values["metadata_source"].(string)
	sourceRunID, _ := msg.Values["source_run_id"].(string)
	switch metadataSource {
	case "latest":
		if sourceRunID != "" {
			return model.SingleNodeRunInput{}, fmt.Errorf("%w: message %s sets source_run_id but metadata_source=latest forbids it", events.ErrPermanent, msg.ID)
		}
	case "snapshot_of_run":
		if sourceRunID == "" {
			return model.SingleNodeRunInput{}, fmt.Errorf("%w: message %s metadata_source=snapshot_of_run requires source_run_id", events.ErrPermanent, msg.ID)
		}
		if err := validUUIDValue(msg.ID, "source_run_id", sourceRunID); err != nil {
			return model.SingleNodeRunInput{}, err
		}
	default:
		return model.SingleNodeRunInput{}, fmt.Errorf("%w: message %s has invalid metadata_source %q (want 'latest' or 'snapshot_of_run')", events.ErrPermanent, msg.ID, metadataSource)
	}

	return model.SingleNodeRunInput{
		RunID:          scheduleID,
		ScheduleName:   scheduleName,
		ServiceName:    serviceName,
		SchemaName:     schemaName,
		TableName:      tableName,
		MetadataSource: metadataSource,
		SourceRunID:    sourceRunID,
		InitiatedBy:    optionalUserField(msg, "initiated_by"),
	}, nil
}
