package handlers

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/commandrequest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// A single-node call that names no acknowledged dependent has the fingerprint
// every call had before a call could name them, so a request id sent then
// replays; naming any adds one part that tells another list apart.
func TestSingleNodeFingerprint_TheAcknowledgedObjectsAreOnePartOnlyWhenNamed(t *testing.T) {
	in := TriggerSingleNodeRunInput{
		Target:         run.NodeID{ServiceName: "svc", SchemaName: "raw", TableName: "fx"},
		MetadataSource: run.MetadataSourceLatest, Operation: model.OperationFullRefresh,
	}
	fingerprint := func(in TriggerSingleNodeRunInput) string {
		return commandrequest.Fingerprint(commandTriggerSingleNodeRun, singleNodeFingerprintParts(in)...)
	}
	assert.Equal(t, commandrequest.Fingerprint("trigger_single_node_run", "svc.raw.fx", "latest", "", "full_refresh"), fingerprint(in))
	in.AcknowledgedDependents = []string{}
	assert.Equal(t, commandrequest.Fingerprint("trigger_single_node_run", "svc.raw.fx", "latest", "", "full_refresh"), fingerprint(in),
		"an empty list is none")

	in.AcknowledgedDependents = []string{"bi.a", "bi.b"}
	assert.Equal(t, commandrequest.Fingerprint("trigger_single_node_run", "svc.raw.fx", "latest", "", "full_refresh", "ack=bi.a,bi.b"), fingerprint(in))
	other := in
	other.AcknowledgedDependents = []string{"bi.a"}
	assert.NotEqual(t, fingerprint(in), fingerprint(other))

	source := uuid.New()
	in.SourceRunID = &source
	in.AcknowledgedDependents = nil
	assert.Equal(t, commandrequest.Fingerprint("trigger_single_node_run", "svc.raw.fx", "latest", source.String(), "full_refresh"), fingerprint(in))
}
