package redis

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/serialization"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParsePromotedSeedsRun translates a trigger.promoted_seeds:v1 XMessage into a
// PromotedSeedsRunInput.
//
// `nodes` arrives as a JSON array in a single field: the publisher encodes any
// non-scalar outbox payload value as JSON before XADD, since a Redis stream
// field holds only a scalar.
//
// Every violation is an events.ErrPermanent-wrapped error so the consumer
// dead-letters the poison message instead of retrying a payload that can never
// parse.
func ParsePromotedSeedsRun(msg goredis.XMessage) (model.PromotedSeedsRunInput, error) {
	scheduleID, err := requireUUIDField(msg, "schedule_id")
	if err != nil {
		return model.PromotedSeedsRunInput{}, err
	}
	scheduleName, err := requireString(msg, "schedule_name")
	if err != nil {
		return model.PromotedSeedsRunInput{}, err
	}
	releaseID, err := requireString(msg, "release_id")
	if err != nil {
		return model.PromotedSeedsRunInput{}, err
	}

	rawNodes, ok := msg.Values["nodes"].(string)
	if !ok || rawNodes == "" {
		return model.PromotedSeedsRunInput{}, fmt.Errorf("%w: message %s has missing or empty nodes field", events.ErrPermanent, msg.ID)
	}
	var nodeDTOs []serialization.PromotedSeedsNodeDTO
	if err := json.Unmarshal([]byte(rawNodes), &nodeDTOs); err != nil {
		return model.PromotedSeedsRunInput{}, fmt.Errorf("%w: message %s has malformed nodes field: %v", events.ErrPermanent, msg.ID, err)
	}
	nodes := serialization.PromotedSeedsNodesToDomain(nodeDTOs)
	// An empty set would snapshot to an empty projection and finalise a run that
	// built nothing. state does not emit one, so an empty set here means a
	// malformed message rather than a promotion with no seeds.
	if len(nodes) == 0 {
		return model.PromotedSeedsRunInput{}, fmt.Errorf("%w: message %s carries an empty nodes list", events.ErrPermanent, msg.ID)
	}
	for i, n := range nodes {
		if n.NodeType == "" || n.ImageTag == "" {
			return model.PromotedSeedsRunInput{}, fmt.Errorf(
				"%w: message %s node %d is missing the release-pinned node_type or image_tag", events.ErrPermanent, msg.ID, i)
		}
		if n.ServiceName == "" || n.SchemaName == "" || n.TableName == "" {
			return model.PromotedSeedsRunInput{}, fmt.Errorf(
				"%w: message %s node %d is missing service_name, schema_name, or table_name", events.ErrPermanent, msg.ID, i)
		}
	}

	promotionSeq, err := optionalPromotionSeq(msg)
	if err != nil {
		return model.PromotedSeedsRunInput{}, err
	}

	return model.PromotedSeedsRunInput{
		RunID:        scheduleID,
		ScheduleName: scheduleName,
		ReleaseID:    releaseID,
		Nodes:        nodes,
		InitiatedBy:  optionalUserField(msg, "initiated_by"),
		PromotionSeq: promotionSeq,
	}, nil
}

// optionalPromotionSeq reads promotion_seq, the live promotion seq state
// planned the run against. The field is optional and 0 stands for its absence;
// present, it must be a positive decimal integer, and anything else is
// permanent.
func optionalPromotionSeq(msg goredis.XMessage) (int64, error) {
	raw, present := msg.Values["promotion_seq"]
	if !present {
		return 0, nil
	}
	s, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("%w: message %s has a non-string promotion_seq field", events.ErrPermanent, msg.ID)
	}
	seq, err := strconv.ParseInt(s, 10, 64)
	if err != nil || seq < 1 {
		return 0, fmt.Errorf("%w: message %s has promotion_seq %q, want a positive integer", events.ErrPermanent, msg.ID, s)
	}
	return seq, nil
}
