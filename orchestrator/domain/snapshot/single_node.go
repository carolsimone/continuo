package snapshot

import (
	"context"
	"fmt"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// SingleNode is the Feature 4 selector — produces exactly one TaskProjection
// for the identified target node. Two modes:
//   - "latest":          metadata pair from latest :Table topology.
//   - "snapshot_of_run": metadata pair from source :Run's :EXECUTES edge.
type SingleNode struct {
	ServiceName    string
	SchemaName     string
	TableName      string
	MetadataSource string // "latest" | "snapshot_of_run"
}

func (s SingleNode) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	if s.ServiceName == "" || s.SchemaName == "" || s.TableName == "" {
		return nil, fmt.Errorf("SingleNode: service_name, schema_name, and table_name are required")
	}
	fqn := FQN{Service: s.ServiceName, Schema: s.SchemaName, Table: s.TableName}
	switch s.MetadataSource {
	case "latest":
		row, ok, err := r.LoadSingleLatestTable(ctx, fqn)
		if err != nil {
			return nil, fmt.Errorf("SingleNode latest: %w", err)
		}
		if !ok {
			return nil, ErrTargetNotFound
		}
		if p.Operation == string(pkgModel.OperationTest) && (!row.TestCountKnown || row.TestCount <= 0) {
			return nil, ErrNoTests
		}
		return []TaskProjection{toSingleNodeProjection(fqn, row)}, nil
	case "snapshot_of_run":
		if p.SourceRunID == nil {
			return nil, fmt.Errorf("SingleNode: snapshot_of_run mode requires SourceRunID")
		}
		row, ok, err := r.LoadSingleTableFromSourceRun(ctx, p.SourceRunID.String(), fqn)
		if err != nil {
			return nil, fmt.Errorf("SingleNode stale: %w", err)
		}
		if !ok {
			return nil, ErrTargetNotFound
		}
		if p.Operation == string(pkgModel.OperationTest) && (!row.TestCountKnown || row.TestCount <= 0) {
			return nil, ErrNoTests
		}
		return []TaskProjection{toSingleNodeProjection(fqn, row)}, nil
	default:
		return nil, fmt.Errorf("SingleNode: invalid MetadataSource %q (want 'latest' or 'snapshot_of_run')", s.MetadataSource)
	}
}

func toSingleNodeProjection(fqn FQN, row LatestTableRow) TaskProjection {
	return TaskProjection{
		TaskID:          uuid.New(),
		ServiceName:     fqn.Service,
		SchemaName:      fqn.Schema,
		TableName:       fqn.Table,
		ScheduleName:    row.ScheduleName,
		NodeType:        row.NodeType,
		InitialStatus:   "PENDING",
		ImageTag:        row.ImageTag,
		ManifestVersion: row.ManifestVersion,
		ContentHash:     row.ContentHash,
		TestCount:       row.TestCount,
		TestCountKnown:  row.TestCountKnown,
		MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
	}
}
