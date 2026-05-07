package run

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
)

// ErrTargetNotFound is returned by SnapshotSingleNodeRun when the requested
// target node (latest mode) or :EXECUTES edge of the source run (stale
// mode) cannot be located. Callers should convert this into a run.failed:v1
// outcome rather than retrying.
var ErrTargetNotFound = errors.New("single-node run target not found")

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

	// SnapshotSingleNodeRun creates a one-task :Run + :Task + :EXECUTES edge
	// for a single-node run.
	//
	// metadataSource: "latest" reads :TopologyRoot + :Table for the metadata
	// pair; "snapshot_of_run" reads the :EXECUTES edge of :Run{run_id: sourceRunID}.
	// Returns the created task_id, image_tag, manifest_version, and the
	// node_type (used to compose the dispatch event).
	//
	// Returns ErrTargetNotFound if the requested target doesn't exist in the
	// chosen source — the orchestrator handler converts this into run.failed:v1.
	SnapshotSingleNodeRun(
		ctx context.Context,
		runID, scheduleName string,
		sourceRunID *uuid.UUID,
		serviceName, schemaName, tableName string,
		metadataSource string,
	) (taskID, imageTag, manifestVersion, nodeType string, err error)

	// Read-side: queries (CQRS — called directly, not through aggregate)
	GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error)
	ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error)
	GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error)
}
