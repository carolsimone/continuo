package snapshot_test

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

// fakeTopologyReader is a hand-rolled in-memory implementation used by the
// pure-Go selector tests. Each field is pre-loaded by the test; methods just
// look up. Errors can be injected per method via the *Err fields.
type fakeTopologyReader struct {
	LatestDAG            map[snapshot.FQN]snapshot.LatestTableRow
	SourceTasks          map[string]map[snapshot.FQN]snapshot.SourceTaskRow
	DescendantsLatest    map[snapshot.FQN][]snapshot.FQN
	DescendantsSource    map[string]map[snapshot.FQN][]snapshot.FQN
	ImmDescendantsLatest map[snapshot.FQN][]snapshot.FQN
	ImmDescendantsSource map[string]map[snapshot.FQN][]snapshot.FQN
	SingleLatest         map[snapshot.FQN]snapshot.LatestTableRow
	SingleFromSourceRun  map[string]map[snapshot.FQN]snapshot.LatestTableRow
	// SourceRunOperation keyed by sourceRunID; missing key returns "" (mirrors
	// the adapter's "not found" behaviour).
	SourceRunOperationByID map[string]string

	LatestDAGErr           error
	SourceTasksErr         error
	DescendantsLatestErr   error
	DescendantsSourceErr   error
	SingleLatestErr        error
	SingleFromSourceRunErr error
	SourceRunOperationErr  error

	// Per-method call counters let tests assert that a selector pass issues a
	// single batched reader call rather than one per FQN.
	DescendantsLatestCalls    int
	DescendantsSourceCalls    int
	ImmDescendantsLatestCalls int
	ImmDescendantsSourceCalls int
}

func (f *fakeTopologyReader) LoadLatestSourceDAG(ctx context.Context, scheduleName string) (map[snapshot.FQN]snapshot.LatestTableRow, error) {
	if f.LatestDAGErr != nil {
		return nil, f.LatestDAGErr
	}
	if f.LatestDAG == nil {
		return map[snapshot.FQN]snapshot.LatestTableRow{}, nil
	}
	return f.LatestDAG, nil
}

func (f *fakeTopologyReader) LoadSourceTasks(ctx context.Context, sourceRunID string) (map[snapshot.FQN]snapshot.SourceTaskRow, error) {
	if f.SourceTasksErr != nil {
		return nil, f.SourceTasksErr
	}
	if m, ok := f.SourceTasks[sourceRunID]; ok {
		return m, nil
	}
	return map[snapshot.FQN]snapshot.SourceTaskRow{}, nil
}

func (f *fakeTopologyReader) DescendantsInLatestTopologyBatch(ctx context.Context, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	f.DescendantsLatestCalls++
	if f.DescendantsLatestErr != nil {
		return nil, f.DescendantsLatestErr
	}
	out := make(map[snapshot.FQN][]snapshot.FQN, len(starts))
	for _, s := range starts {
		out[s] = f.DescendantsLatest[s]
	}
	return out, nil
}

func (f *fakeTopologyReader) DescendantsInSourceRunBatch(ctx context.Context, sourceRunID string, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	f.DescendantsSourceCalls++
	if f.DescendantsSourceErr != nil {
		return nil, f.DescendantsSourceErr
	}
	out := make(map[snapshot.FQN][]snapshot.FQN, len(starts))
	if m, ok := f.DescendantsSource[sourceRunID]; ok {
		for _, s := range starts {
			out[s] = m[s]
		}
	}
	return out, nil
}

func (f *fakeTopologyReader) ImmediateDescendantsInLatestTopologyBatch(ctx context.Context, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	f.ImmDescendantsLatestCalls++
	if f.DescendantsLatestErr != nil {
		return nil, f.DescendantsLatestErr
	}
	out := make(map[snapshot.FQN][]snapshot.FQN, len(starts))
	for _, s := range starts {
		out[s] = f.ImmDescendantsLatest[s]
	}
	return out, nil
}

func (f *fakeTopologyReader) ImmediateDescendantsInSourceRunBatch(ctx context.Context, sourceRunID string, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	f.ImmDescendantsSourceCalls++
	if f.DescendantsSourceErr != nil {
		return nil, f.DescendantsSourceErr
	}
	out := make(map[snapshot.FQN][]snapshot.FQN, len(starts))
	if m, ok := f.ImmDescendantsSource[sourceRunID]; ok {
		for _, s := range starts {
			out[s] = m[s]
		}
	}
	return out, nil
}

func (f *fakeTopologyReader) LoadSingleLatestTable(ctx context.Context, fqn snapshot.FQN) (snapshot.LatestTableRow, bool, error) {
	if f.SingleLatestErr != nil {
		return snapshot.LatestTableRow{}, false, f.SingleLatestErr
	}
	row, ok := f.SingleLatest[fqn]
	return row, ok, nil
}

func (f *fakeTopologyReader) LoadSingleTableFromSourceRun(ctx context.Context, sourceRunID string, fqn snapshot.FQN) (snapshot.LatestTableRow, bool, error) {
	if f.SingleFromSourceRunErr != nil {
		return snapshot.LatestTableRow{}, false, f.SingleFromSourceRunErr
	}
	if m, ok := f.SingleFromSourceRun[sourceRunID]; ok {
		row, hit := m[fqn]
		return row, hit, nil
	}
	return snapshot.LatestTableRow{}, false, nil
}

func (f *fakeTopologyReader) SourceRunOperation(ctx context.Context, sourceRunID string) (string, error) {
	if f.SourceRunOperationErr != nil {
		return "", f.SourceRunOperationErr
	}
	return f.SourceRunOperationByID[sourceRunID], nil
}
