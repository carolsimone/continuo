package schedule

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A schedule with no cron has nothing to remove, and removing it is a success:
// the schedule runs only when triggered, as it did.
func TestRemoveCron_OfAManualScheduleSucceeds(t *testing.T) {
	fake := &fakeCron{}

	stdout, stderr, exit := runCron(t, NewRemoveCronCommand, fake, []string{"e2e-schedule"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	assert.Equal(t, []string{"e2e-schedule"}, fake.deletes)
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &out))
	assert.Equal(t, "e2e-schedule", out["schedule_name"])
	assert.Equal(t, false, out["has_cron"])
}

func TestRemoveCron_SendsTheNameAsTypedAndMakesOneCall(t *testing.T) {
	fake := &fakeCron{}

	_, _, exit := runCron(t, NewRemoveCronCommand, fake, []string{"Daily"}, false, "")

	require.Equal(t, 0, exit)
	assert.Equal(t, []string{"Daily"}, fake.deletes, "names are case-sensitive and are not folded")
	assert.Equal(t, []string{"delete"}, fake.calls, "a delete needs no read")
}

func TestRemoveCron_ForwardsTheActor(t *testing.T) {
	fake := &fakeCron{}

	_, _, exit := runCron(t, NewRemoveCronCommand, fake, []string{"daily"}, false, "okta.example.com|alice")

	require.Equal(t, 0, exit)
	assert.Equal(t, []string{"okta.example.com|alice"}, fake.actors)
}

func TestRemoveCron_WrongArgumentCountExits2(t *testing.T) {
	for _, args := range [][]string{{}, {"daily", "extra"}, {" "}} {
		fake := &fakeCron{}

		stdout, _, exit := runCron(t, NewRemoveCronCommand, fake, args, false, "")

		assert.Equal(t, 2, exit, "args %q", args)
		assert.Equal(t, output.CodeUsage, errorOf(t, stdout).Code)
		assert.Empty(t, fake.calls)
	}
}

func TestRemoveCron_MaintenanceExits7(t *testing.T) {
	fake := &fakeCron{deleteErr: maintenanceRefusal()}

	stdout, _, exit := runCron(t, NewRemoveCronCommand, fake, []string{"daily"}, false, "")

	assert.Equal(t, 7, exit)
	assert.Equal(t, output.CodeMaintenance, errorOf(t, stdout).Code)
}

func TestRemoveCron_UnavailableExits5(t *testing.T) {
	fake := &fakeCron{deleteErr: status.Error(codes.Unavailable, "down")}

	_, _, exit := runCron(t, NewRemoveCronCommand, fake, []string{"daily"}, false, "")

	assert.Equal(t, 5, exit)
}

func TestRemoveCron_HumanModeWritesOneLineToStderr(t *testing.T) {
	fake := &fakeCron{}

	stdout, stderr, exit := runCron(t, NewRemoveCronCommand, fake, []string{"daily"}, true, "")

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily")
	assert.Contains(t, stderr, "only when triggered")
}

func TestRemoveCron_OutputMatchesItsSchema(t *testing.T) {
	stdout, _, exit := runCron(t, NewRemoveCronCommand, &fakeCron{}, []string{"daily"}, false, "")
	require.Equal(t, 0, exit)

	assertEmitsItsSchema(t, NewRemoveCronCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{}), stdout)
}
