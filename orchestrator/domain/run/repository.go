package run

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
)

type Repository interface {
	// Write-side: snapshot and mutation

	// SnapshotGraph stamps a :Run node with kind (run-level discriminator) and
	// optionally source_run_id (the schedule_id of the parent run this one
	// derives from; nil for cron/trigger). See adapters/neo4j/run_repository.go
	// for the Cypher details.
	SnapshotGraph(ctx context.Context, runID, scheduleName, kind string, sourceRunID *uuid.UUID) error
	UpdateNodeStatus(ctx context.Context, runID, scheduleName, schema, tableName, status string) error
	GetReadyDownstream(ctx context.Context, runID, scheduleName, schema, tableName string) ([]*DownstreamNode, error)
	CheckScheduleCompletion(ctx context.Context, runID, scheduleName string) (isComplete bool, hasFailed bool, err error)
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
	GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*ScheduleInitNodes, error)
	GetTransitiveDownstream(ctx context.Context, scheduleName, schema, tableName string) ([]*domain.TableNode, error)
	GetNodeType(ctx context.Context, schema, tableName string) (string, error)
	GetNodeServiceName(ctx context.Context, schema, tableName string) (string, error)
	GetTaskIDForNode(ctx context.Context, runID, serviceName, schemaName, tableName string) (string, error)
	GetSkippedDownstreamTaskIDs(ctx context.Context, runID, schemaName, tableName string) ([]string, error)
	MarkPendingDownstreamSkipped(ctx context.Context, runID, scheduleName, schemaName, tableName string) ([]*CascadedFailureNode, error)
	ResetSkippedDownstreamToPending(ctx context.Context, runID, schemaName, tableName string) error
	GetNodeEdgeData(ctx context.Context, runID, schemaName, tableName string) (manifestVersion, imageTag string, err error)

	// Read-side: queries (CQRS — called directly, not through aggregate)
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error)
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
}
