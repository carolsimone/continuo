package snapshot

import (
	"context"
	"fmt"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// LatestFullDAG is the cron / trigger selector. It returns every active :Table
// in the schedule plus their upstream dependencies (typically dbt-seeds in
// other schedules). All projected tasks are PENDING and pinned to (image_tag,
// manifest_version) from the latest :Table topology at snapshot time.
//
// It also classifies the dispatch frontier on each task's ReadyToDispatch flag:
// a node is ready when it has no in-DAG upstream (nothing in the DAG depends on
// it being produced first). This is exactly the seeds-first-else-roots ordering
// — seeds have no upstream and dispatch first; roots that depend on a seed wait
// for it, while roots with no upstream at all dispatch immediately. The run
// aggregate dispatches the rest via NodeUnblocked as upstreams complete.
type LatestFullDAG struct{}

func (LatestFullDAG) SelectTasks(ctx context.Context, r TopologyReader, p Params) ([]TaskProjection, error) {
	rows, err := r.LoadLatestSourceDAG(ctx, p.ScheduleName)
	if err != nil {
		return nil, fmt.Errorf("LatestFullDAG: %w", err)
	}

	// Whole-DAG test runs are edgeless: every node with tests dispatches
	// `dbt test` independently, with no blocking/frontier ordering at all.
	if p.Operation == string(pkgModel.OperationTest) {
		projection := make([]TaskProjection, 0, len(rows))
		for f, row := range rows {
			// Flat fan-out projects only nodes with a KNOWN, positive test count.
			// An unknown count (topology predating test_count capture) or a known
			// zero is excluded: we never dispatch a speculative `dbt test` that
			// would no-op on a node we cannot confirm has tests.
			if !row.TestCountKnown || row.TestCount <= 0 {
				continue
			}
			projection = append(projection, TaskProjection{
				TaskID:             uuid.New(),
				ServiceName:        f.Service,
				SchemaName:         f.Schema,
				TableName:          f.Table,
				ScheduleName:       row.ScheduleName,
				NodeType:           row.NodeType,
				InitialStatus:      "PENDING",
				ImageTag:           row.ImageTag,
				ManifestVersion:    row.ManifestVersion,
				DBTUniqueID:        row.DBTUniqueID,
				RuntimeManifestRef: row.RuntimeManifestRef,
				TestCount:          row.TestCount,
				TestCountKnown:     row.TestCountKnown,
				MaxRetries:         pkgEvents.DefaultTaskMaxRetries,
				ReadyToDispatch:    true, // edgeless: no blocking frontier for tests
			})
		}
		if len(projection) == 0 {
			// Every node was gated (known-zero or unknown test count): there is
			// nothing to test. Surface the benign no-tests sentinel so state
			// finalizes the run as `skipped`, not `failed`. Returning an empty
			// projection here would degrade to ErrEmptyProjection at the service
			// layer, which is reserved for a broken operation=run DAG.
			return nil, ErrNoTests
		}
		return projection, nil
	}

	// Compute the dispatch frontier. A node is blocked when it is the immediate
	// dependent of another node in this DAG; the frontier is everything not
	// blocked. One batched reader call resolves every node's immediate dependents.
	starts := make([]FQN, 0, len(rows))
	inDAG := make(map[FQN]struct{}, len(rows))
	for f := range rows {
		starts = append(starts, f)
		inDAG[f] = struct{}{}
	}
	immBySeed, err := r.ImmediateDescendantsInLatestTopologyBatch(ctx, starts)
	if err != nil {
		return nil, fmt.Errorf("LatestFullDAG: frontier: %w", err)
	}
	blocked := make(map[FQN]struct{})
	for _, f := range starts {
		for _, d := range immBySeed[f] {
			if _, ok := inDAG[d]; ok {
				blocked[d] = struct{}{}
			}
		}
	}

	projection := make([]TaskProjection, 0, len(rows))
	for f, row := range rows {
		_, isBlocked := blocked[f]
		projection = append(projection, TaskProjection{
			TaskID:             uuid.New(),
			ServiceName:        f.Service,
			SchemaName:         f.Schema,
			TableName:          f.Table,
			ScheduleName:       row.ScheduleName,
			NodeType:           row.NodeType,
			InitialStatus:      "PENDING",
			ImageTag:           row.ImageTag,
			ManifestVersion:    row.ManifestVersion,
			DBTUniqueID:        row.DBTUniqueID,
			RuntimeManifestRef: row.RuntimeManifestRef,
			TestCount:          row.TestCount,
			TestCountKnown:     row.TestCountKnown,
			MaxRetries:         pkgEvents.DefaultTaskMaxRetries,
			ReadyToDispatch:    !isBlocked,
		})
	}
	return projection, nil
}
