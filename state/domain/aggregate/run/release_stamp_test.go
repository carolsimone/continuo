package run_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stampNow = time.Date(2026, 10, 14, 23, 0, 0, 0, time.UTC)

func TestReleaseStamp_ZeroNamesNoRelease(t *testing.T) {
	assert.True(t, run.ReleaseStamp{}.IsZero())
	assert.False(t, run.ReleaseStamp{ReleaseID: "rel-5", PromotionSeq: 12}.IsZero())
}

func TestNewPendingRun_CarriesItsStamp(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-5", PromotionSeq: 12}
	r, _, err := run.NewPendingRun("daily", run.KindCron, nil, "system", nil, model.OperationRun, stamp, stampNow)
	require.NoError(t, err)
	assert.Equal(t, stamp, r.Release())
	assert.Equal(t, "rel-5", r.ReleaseID())
}

func TestNewDerivedRun_CarriesTheStampItIsGiven(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}
	for _, kind := range []run.Kind{run.KindRerun, run.KindRebase} {
		r, _, err := run.NewDerivedRun("daily", kind, uuid.New(), stamp, "alice", model.OperationRun, stampNow)
		require.NoError(t, err)
		assert.Equal(t, stamp, r.Release(), string(kind))
	}
}

func TestNewSingleNodeRun_CarriesItsStamp(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-5", PromotionSeq: 12}
	r, _, err := run.NewSingleNodeRun("single-node-run-ab12cd34",
		run.NodeID{ServiceName: "svc", SchemaName: "raw", TableName: "orders"},
		run.MetadataSourceLatest, model.OperationRun, nil, "alice", stamp, stampNow)
	require.NoError(t, err)
	assert.Equal(t, stamp, r.Release())
}

func TestNewPromotedSeedsRun_NeedsARelease(t *testing.T) {
	nodes := []run.SeedNode{{NodeID: run.NodeID{ServiceName: "svc", SchemaName: "raw", TableName: "orders"}, NodeType: "dbt-seed", ImageTag: "svc:1"}}
	_, _, err := run.NewPromotedSeedsRun(uuid.New(), "promote-seed-ab12cd34", run.ReleaseStamp{PromotionSeq: 4}, nodes, stampNow)
	assert.ErrorIs(t, err, run.ErrReleaseIDRequired)

	stamp := run.ReleaseStamp{ReleaseID: "rel-4", PromotionSeq: 4}
	r, evt, err := run.NewPromotedSeedsRun(uuid.New(), "promote-seed-ab12cd34", stamp, nodes, stampNow)
	require.NoError(t, err)
	assert.Equal(t, stamp, r.Release())
	assert.Equal(t, int64(4), evt.(run.PromotedSeedsRunRequested).PromotionSeq)
}

func TestHydrateRelease_RestoresTheStamp(t *testing.T) {
	r, _, err := run.NewPendingRun("daily", run.KindCron, nil, "system", nil, model.OperationRun, run.ReleaseStamp{}, stampNow)
	require.NoError(t, err)
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 20})
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 20}, r.Release())
}
