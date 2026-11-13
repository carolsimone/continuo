package events_test

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTaskDispatch_CommandCarriesTheContentHash(t *testing.T) {
	d := events.TaskDispatch{TenantID: "default", RunID: uuid.New(), TaskID: uuid.New(), ContentHash: "sha256:91d4c0"}

	assert.Equal(t, "sha256:91d4c0", d.Command().ContentHash)
	assert.Empty(t, events.TaskDispatch{}.Command().ContentHash)
}

func TestTaskDispatch_CommandCarriesTheSeedPreflight(t *testing.T) {
	pf := &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur"}
	d := events.TaskDispatch{TenantID: "default", RunID: uuid.New(), TaskID: uuid.New(), SeedPreflight: pf}

	assert.Equal(t, pf, d.Command().SeedPreflight)
	assert.Nil(t, events.TaskDispatch{}.Command().SeedPreflight)
}
