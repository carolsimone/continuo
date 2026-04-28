package run

import (
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

type Run struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string
	CreatedAt      time.Time
	CompletedAt    *time.Time
}

type ScheduleInitNodes struct {
	AllNodes  []*domain.TableNode
	RootNodes []*domain.TableNode
	SeedNodes []*domain.TableNode
}

type DownstreamNode struct {
	ServiceName     string
	SchemaName      string
	TableName       string
	NodeType        string
	ScheduleName    string
	ManifestVersion string
	ImageTag        string
}

// CascadedFailureNode holds the identifying info for a downstream node
// that was marked FAILED as a result of an upstream failure.
type CascadedFailureNode struct {
	TaskID      string
	SchemaName  string
	TableName   string
	ServiceName string
}
