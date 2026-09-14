package model

import "github.com/google/uuid"

// validationSyntheticIDNamespace seeds the deterministic synthetic
// task_id/schedule_id of validation rows. Validation deploys have no real
// task/schedule identity, but those columns are NOT NULL, so we derive stable
// UUIDv5 values from the (release_id, node_id) business key. This namespace is
// IMMUTABLE — changing it remaps every persisted validation row's synthetic
// task/schedule IDs and breaks correlation with rows already on disk and with
// the node.deployed:v1 trigger keyed off the same IDs.
var validationSyntheticIDNamespace = uuid.MustParse("8f8d2b7a-2f1e-4c6a-9d3b-6a7c1e0f4d22")

// ValidationSyntheticIDs derives the deterministic task/schedule UUIDs for a
// validation deployment from (releaseID, nodeID). task_id/schedule_id are NOT
// NULL in executor_deployments; validation rows have no real task/schedule, so
// these synthetic IDs satisfy the columns and key the node.deployed trigger the
// dispatcher emits so k8s-controller status-checks the validation Job. The NUL
// separator keeps the two business keys unambiguous across boundaries.
func ValidationSyntheticIDs(releaseID, nodeID string) (taskID, scheduleID uuid.UUID) {
	taskID = uuid.NewSHA1(validationSyntheticIDNamespace, []byte("task:"+releaseID+"\x00"+nodeID))
	scheduleID = uuid.NewSHA1(validationSyntheticIDNamespace, []byte("schedule:"+releaseID+"\x00"+nodeID))
	return taskID, scheduleID
}
