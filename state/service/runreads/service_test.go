package runreads_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/runreads"
)

type fakeReads struct {
	tenants       []string
	limit, offset int
}

var _ repository.RunReadRepository = (*fakeReads)(nil)

func (f *fakeReads) GetRunGraph(_ context.Context, tenantID string, _ uuid.UUID) (projection.RunGraph, error) {
	f.tenants = append(f.tenants, tenantID)
	return projection.RunGraph{RunPromotionSeq: 4, LatestPromotionSeq: 7}, nil
}

func (f *fakeReads) ListCompletedRuns(_ context.Context, tenantID, _ string, limit, offset int) ([]projection.RunSummary, int, error) {
	f.tenants = append(f.tenants, tenantID)
	f.limit, f.offset = limit, offset
	return nil, 0, nil
}

func (f *fakeReads) ListActiveRunDrifts(_ context.Context, tenantID string) (int64, []projection.ActiveRunDrift, error) {
	f.tenants = append(f.tenants, tenantID)
	return 7, nil, nil
}

func (f *fakeReads) LastSuccessDurations(context.Context, string, []run.NodeID) (map[run.NodeID]time.Duration, error) {
	return nil, nil
}

// Every read reads the rows of the tenant it is given.
func TestService_ReadsTheTenantItIsGiven(t *testing.T) {
	f := &fakeReads{}
	s := runreads.New(f)

	g, err := s.GetRunGraph(context.Background(), "acme", uuid.New())
	require.NoError(t, err)
	assert.Equal(t, int64(4), g.RunPromotionSeq)
	_, _, err = s.ListActiveRunDrifts(context.Background(), "acme")
	require.NoError(t, err)
	_, _, err = s.ListRuns(context.Background(), pkgevents.DefaultTenantID, "daily", 10, 0)
	require.NoError(t, err)

	assert.Equal(t, []string{"acme", "acme", pkgevents.DefaultTenantID}, f.tenants)
}

func TestService_ClampsThePage(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		size, offset          int
		wantLimit, wantOffset int
	}{
		{"zero size gets the default", 0, 0, runreads.DefaultPageSize, 0},
		{"negative size gets the default", -3, 0, runreads.DefaultPageSize, 0},
		{"oversized page is capped", 500, 10, runreads.MaxPageSize, 10},
		{"negative offset becomes zero", 20, -5, 20, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeReads{}
			_, _, err := runreads.New(f).ListRuns(context.Background(), pkgevents.DefaultTenantID, "daily", tc.size, tc.offset)
			require.NoError(t, err)
			assert.Equal(t, tc.wantLimit, f.limit)
			assert.Equal(t, tc.wantOffset, f.offset)
		})
	}
}
