package run_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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
	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, "daily", run.KindCron, nil, "system", nil, model.OperationRun, stamp, stampNow)
	require.NoError(t, err)
	assert.Equal(t, stamp, r.Release())
	assert.Equal(t, "rel-5", r.ReleaseID())
}

func TestNewDerivedRun_CarriesTheStampItIsGiven(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}
	for _, kind := range []run.Kind{run.KindRerun, run.KindRebase} {
		r, err := run.NewDerivedRun(pkgevents.DefaultTenantID, "daily", kind, uuid.New(), stamp, "alice", model.OperationRun, stampNow)
		require.NoError(t, err)
		assert.Equal(t, stamp, r.Release(), string(kind))
	}
}

func TestNewSingleNodeRun_CarriesItsStamp(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-5", PromotionSeq: 12}
	r, err := run.NewSingleNodeRun(pkgevents.DefaultTenantID, "single-node-run-ab12cd34", model.OperationRun, nil, "alice", stamp, stampNow)
	require.NoError(t, err)
	assert.Equal(t, stamp, r.Release())
}

func TestNewPromotedSeedsRun_NeedsARelease(t *testing.T) {
	_, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-ab12cd34", run.ReleaseStamp{PromotionSeq: 4}, stampNow)
	assert.ErrorIs(t, err, run.ErrReleaseIDRequired)

	stamp := run.ReleaseStamp{ReleaseID: "rel-4", PromotionSeq: 4}
	r, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-ab12cd34", stamp, stampNow)
	require.NoError(t, err)
	assert.Equal(t, stamp, r.Release())
}

func TestHydrateRelease_RestoresTheStamp(t *testing.T) {
	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, "daily", run.KindCron, nil, "system", nil, model.OperationRun, run.ReleaseStamp{}, stampNow)
	require.NoError(t, err)
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 20})
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 20}, r.Release())
}
