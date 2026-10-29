package run

import (
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
)

// taskIDNamespace is the UUIDv5 namespace of the task ids TaskIDFor derives.
var taskIDNamespace = uuid.MustParse("5b1f6a3e-2c7d-4f8a-9e0b-3d4c5a6b7e8f")

// TaskIDFor returns the id of the task of run runID at node key, for tenant
// tenantID. The same inputs always give the same id, so a run's tasks are
// named before they are stored and a node is one task per run.
func TaskIDFor(tenantID string, runID uuid.UUID, key topology.NodeKey) uuid.UUID {
	return uuid.NewSHA1(taskIDNamespace, []byte(tenantID+"\x00"+runID.String()+"\x00"+string(key)))
}
