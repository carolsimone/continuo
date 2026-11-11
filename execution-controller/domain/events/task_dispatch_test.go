package events_test

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTaskDispatch_CommandCarriesTheContentHash(t *testing.T) {
	d := events.TaskDispatch{TenantID: "default", RunID: uuid.New(), TaskID: uuid.New(), ContentHash: "sha256:91d4c0"}

	assert.Equal(t, "sha256:91d4c0", d.Command().ContentHash)
	assert.Empty(t, events.TaskDispatch{}.Command().ContentHash)
}
