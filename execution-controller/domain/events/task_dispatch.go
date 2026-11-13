package events

import (
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/google/uuid"
)

// TaskDispatch is the parsed task.dispatch:v1 payload: one attempt of one
// task as run-controller decided it, with run-controller's view of the attempt (KnownStatus).
// Only RunID, TaskID and Attempt are known to be valid; the other fields are
// as run-controller wrote them. TenantID is the tenant of the dispatch's envelope.
type TaskDispatch struct {
	TenantID       string
	RunID          uuid.UUID
	TaskID         uuid.UUID
	Attempt        int
	KnownStatus    pkgmodel.AttemptStatus
	ScheduleName   string
	ServiceName    string
	SchemaName     string
	TableName      string
	NodeType       string
	Operation      string
	ImageTag       string
	ContentHash    string
	SecretRef      string
	JobName        string
	TimeoutSeconds int64
	// SeedPreflight is the check a seed's pod runs against the warehouse
	// before the load that drops its table; nil for every other task.
	SeedPreflight *seedreload.Preflight
}

// Command is the deploy command of the dispatched attempt.
func (d TaskDispatch) Command() command.DeployTask {
	return command.DeployTask{
		TenantID: d.TenantID, TaskID: d.TaskID.String(), RunID: d.RunID.String(), ScheduleName: d.ScheduleName,
		ServiceName: d.ServiceName, SchemaName: d.SchemaName, TableName: d.TableName, JobName: d.JobName,
		NodeType: d.NodeType, ImageTag: d.ImageTag, SecretRef: d.SecretRef, Attempt: d.Attempt,
		Operation: d.Operation, TimeoutSeconds: d.TimeoutSeconds, ContentHash: d.ContentHash,
		SeedPreflight: d.SeedPreflight,
	}
}
