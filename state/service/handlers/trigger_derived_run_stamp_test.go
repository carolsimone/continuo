package handlers

import (
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A rerun runs on its source's release; a rebase names none of its own.
func TestTriggerDerivedRun_StampOfPerKind(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}
	src, _, err := run.NewPendingRun("daily", run.KindCron, nil, "system", nil, model.OperationRun, stamp, time.Now())
	require.NoError(t, err)

	rerun := NewTriggerRerunHandler(slog.Default())
	assert.Equal(t, stamp, rerun.cfg.stampOf(src))

	rebase := NewTriggerRebaseHandler(slog.Default())
	assert.True(t, rebase.cfg.stampOf(src).IsZero())
}
