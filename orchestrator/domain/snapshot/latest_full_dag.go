package snapshot

import (
	"context"
	"fmt"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// LatestFullDAG is the cron / trigger selector. It returns every active :Table
// in the schedule plus their upstream dependencies (typically dbt-seeds in
// other schedules). All projected tasks are PENDING and pinned to (image_tag,
// manifest_version) from the latest :Table topology at snapshot time.
type LatestFullDAG struct{}

func (LatestFullDAG) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	rows, err := r.LoadLatestSourceDAG(ctx, p.ScheduleName)
	if err != nil {
		return nil, fmt.Errorf("LatestFullDAG: %w", err)
	}
	projection := make([]TaskProjection, 0, len(rows))
	for f, row := range rows {
		projection = append(projection, TaskProjection{
			TaskID:          uuid.New(),
			ServiceName:     f.Service,
			SchemaName:      f.Schema,
			TableName:       f.Table,
			ScheduleName:    row.ScheduleName,
			NodeType:        row.NodeType,
			InitialStatus:   "PENDING",
			ImageTag:        row.ImageTag,
			ManifestVersion: row.ManifestVersion,
			MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
		})
	}
	return projection, nil
}
