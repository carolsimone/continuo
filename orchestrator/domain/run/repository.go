package run

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

type Repository interface {
	// Write-side: snapshot and mutation

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

	// Snapshot is the unified per-run snapshot routine (umbrella §3). Selector
	// inside params reads source/topology and returns the projection; the
	// kind-agnostic materialiser writes :Run + :EXECUTES edges in one Cypher tx.
	// Returns the projection so the caller can build downstream outbox events
	// (run.entries.dispatched:v1, query.model:v1) without re-reading Neo4j.
	//
	// Returns snapshot.ErrEmptyProjection if the selector returns zero entries.
	// Returns snapshot.ErrTargetNotFound if a target-resolving selector
	// (e.g. SingleNode) cannot find its target.
	Snapshot(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error)

	// Read-side: queries (CQRS — called directly, not through aggregate)
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error)
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
}
