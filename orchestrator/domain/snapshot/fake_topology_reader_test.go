package snapshot_test

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

// fakeTopologyReader is a hand-rolled in-memory implementation used by the
// pure-Go selector tests. Each field is pre-loaded by the test; methods just
// look up. Errors can be injected per method via the *Err fields.
type fakeTopologyReader struct {
	LatestDAG               map[snapshot.FQN]snapshot.LatestTableRow
	SourceTasks             map[string]map[snapshot.FQN]snapshot.SourceTaskRow
	DescendantsLatest       map[snapshot.FQN][]snapshot.FQN
	DescendantsSource       map[string]map[snapshot.FQN][]snapshot.FQN
	SingleLatest            map[snapshot.FQN]snapshot.LatestTableRow
	SingleFromSourceRun     map[string]map[snapshot.FQN]snapshot.LatestTableRow

	LatestDAGErr            error
	SourceTasksErr          error
	DescendantsLatestErr    error
	DescendantsSourceErr    error
	SingleLatestErr         error
	SingleFromSourceRunErr  error
}

func (f *fakeTopologyReader) LoadLatestSourceDAG(ctx context.Context, scheduleName string) (map[snapshot.FQN]snapshot.LatestTableRow, error) {
	if f.LatestDAGErr != nil { return nil, f.LatestDAGErr }
	if f.LatestDAG == nil { return map[snapshot.FQN]snapshot.LatestTableRow{}, nil }
	return f.LatestDAG, nil
}

func (f *fakeTopologyReader) LoadSourceTasks(ctx context.Context, sourceRunID string) (map[snapshot.FQN]snapshot.SourceTaskRow, error) {
	if f.SourceTasksErr != nil { return nil, f.SourceTasksErr }
	if m, ok := f.SourceTasks[sourceRunID]; ok { return m, nil }
	return map[snapshot.FQN]snapshot.SourceTaskRow{}, nil
}

func (f *fakeTopologyReader) DescendantsInLatestTopology(ctx context.Context, start snapshot.FQN) ([]snapshot.FQN, error) {
	if f.DescendantsLatestErr != nil { return nil, f.DescendantsLatestErr }
	return f.DescendantsLatest[start], nil
}

func (f *fakeTopologyReader) DescendantsInSourceRun(ctx context.Context, sourceRunID string, start snapshot.FQN) ([]snapshot.FQN, error) {
	if f.DescendantsSourceErr != nil { return nil, f.DescendantsSourceErr }
	if m, ok := f.DescendantsSource[sourceRunID]; ok { return m[start], nil }
	return nil, nil
}

func (f *fakeTopologyReader) LoadSingleLatestTable(ctx context.Context, fqn snapshot.FQN) (snapshot.LatestTableRow, bool, error) {
	if f.SingleLatestErr != nil { return snapshot.LatestTableRow{}, false, f.SingleLatestErr }
	row, ok := f.SingleLatest[fqn]
	return row, ok, nil
}

func (f *fakeTopologyReader) LoadSingleTableFromSourceRun(ctx context.Context, sourceRunID string, fqn snapshot.FQN) (snapshot.LatestTableRow, bool, error) {
	if f.SingleFromSourceRunErr != nil { return snapshot.LatestTableRow{}, false, f.SingleFromSourceRunErr }
	if m, ok := f.SingleFromSourceRun[sourceRunID]; ok {
		row, hit := m[fqn]
		return row, hit, nil
	}
	return snapshot.LatestTableRow{}, false, nil
}
