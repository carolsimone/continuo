package handlers

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A rerun runs on its source's release; a rebase runs on the live one.
func TestTriggerDerivedRun_StampOfPerKind(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}
	src, _, err := run.NewPendingRun("daily", run.KindCron, nil, "system", nil, model.OperationRun, stamp, time.Now())
	require.NoError(t, err)
	live := run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}
	u := &uow.FakeUnitOfWork{TopologyRepo: &uow.LiveReleaseOnly{Live: topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}}}

	rerun := NewTriggerRerunHandler(slog.Default())
	got, err := rerun.cfg.stampOf(context.Background(), u, src)
	require.NoError(t, err)
	assert.Equal(t, stamp, got)

	rebase := NewTriggerRebaseHandler(slog.Default())
	got, err = rebase.cfg.stampOf(context.Background(), u, src)
	require.NoError(t, err)
	assert.Equal(t, live, got)
}
