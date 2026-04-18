package command

import (
	"github.com/google/uuid"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// Command is a marker interface for all commands
type Command interface {
	isCommand()
}

// DeployJob represents a command to deploy a K8s Job for query execution
type DeployJob struct {
	TaskID         uuid.UUID
	ScheduleID     uuid.UUID
	ScheduleName   string
	ServiceName    string
	SchemaName     string
	TableName      string
	JobName        string
	NodeType       pkg_model.NodeType
	TaskRetryCount int // task-level retry count carried through from the orchestrator
}

func (DeployJob) isCommand() {}
