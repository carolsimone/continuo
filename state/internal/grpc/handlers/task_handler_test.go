package handlers

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/stretchr/testify/assert"
)

// TestTaskStatusConversion_SkippedRoundTrips guards the regression where a
// cascade-skipped node was reported to clients as UNSPECIFIED: the proto
// TaskStatus enum lacked a SKIPPED member, so domainToProtoTaskStatus fell
// through to the default. Every domain status must survive the round trip.
func TestTaskStatusConversion_SkippedRoundTrips(t *testing.T) {
	cases := []struct {
		domain run.TaskStatus
		proto  statev1.TaskStatus
	}{
		{run.TaskStatusPending, statev1.TaskStatus_TASK_STATUS_PENDING},
		{run.TaskStatusRunning, statev1.TaskStatus_TASK_STATUS_RUNNING},
		{run.TaskStatusSucceeded, statev1.TaskStatus_TASK_STATUS_SUCCEEDED},
		{run.TaskStatusFailed, statev1.TaskStatus_TASK_STATUS_FAILED},
		{run.TaskStatusCancelled, statev1.TaskStatus_TASK_STATUS_CANCELLED},
		{run.TaskStatusSkipped, statev1.TaskStatus_TASK_STATUS_SKIPPED},
	}
	for _, c := range cases {
		assert.Equal(t, c.proto, domainToProtoTaskStatus(c.domain),
			"domain %q must map to its proto enum, not UNSPECIFIED", c.domain)
		assert.Equal(t, c.domain, protoToDomainTaskStatus(c.proto),
			"proto %v must map back to its domain status", c.proto)
	}
}
