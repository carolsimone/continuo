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
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    string
}
