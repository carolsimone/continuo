package run

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRunRebase_SendsTheRequestIDTheReleaseAndTheOption(t *testing.T) {
	t.Setenv("CONTINUO_REQUEST_ID", testRequestID)
	fake := &fakeRunClient{rebaseResp: &runv1.TriggerRebaseResponse{RunId: "new-run", ScheduleName: "daily"}}

	stdout, stderr, exit := executeAs(t, NewRebaseCommand, fake, "alice", testRunID, "rel-9", "true")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	assert.Equal(t, []rebaseCall{{requestID: testRequestID, runID: testRunID, expectedReleaseID: "rel-9", actor: "alice", changedUpstream: true}}, fake.rebaseCalls)
	assert.JSONEq(t, `{"run_id":"new-run","schedule_name":"daily"}`, stdout)
	assert.Equal(t, true, fake.deadlineCalls[0])
}

func TestRunRebase_OmittedOptionIsFalse(t *testing.T) {
	fake := &fakeRunClient{rebaseResp: &runv1.TriggerRebaseResponse{RunId: "new-run", ScheduleName: "daily"}}

	_, _, exit := execute(t, NewRebaseCommand, fake, testRunID, "rel-9")

	require.Equal(t, 0, exit)
	assert.False(t, fake.rebaseCalls[0].changedUpstream)
}

func TestRunRebase_IsMutating(t *testing.T) {
	assert.Equal(t, "true", NewRebaseCommand(nil, nil, nil, nil).Annotations["mutating"])
}

func TestRunRebase_RequiresTheReleaseID(t *testing.T) {
	for name, args := range map[string][]string{
		"missing":        {testRunID},
		"blank":          {testRunID, ""},
		"blank run id":   {"", "rel-9"},
		"no arguments":   {},
		"extra argument": {testRunID, "rel-9", "true", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeRunClient{}

			stdout, _, exit := execute(t, NewRebaseCommand, fake, args...)

			assert.Equal(t, 2, exit)
			var env map[string]output.CLIError
			require.NoError(t, json.Unmarshal([]byte(stdout), &env))
			assert.Equal(t, output.CodeUsage, env["error"].Code)
			assert.Empty(t, fake.rebaseCalls, "no RPC is sent")
		})
	}
}

func TestRunRebase_AMovedReleaseSaysPreviewAgain(t *testing.T) {
	fake := &fakeRunClient{rebaseErr: status.Error(codes.FailedPrecondition, "the live release changed; preview again")}

	stdout, _, exit := execute(t, NewRebaseCommand, fake, testRunID, "rel-9")

	assert.Equal(t, 4, exit, "a failed precondition keeps the conflict exit code")
	var env struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeConflict, env.Error.Code)
	assert.Contains(t, env.Error.Message, "the live release changed")
	assert.Contains(t, env.Error.Message, "continuo run rebase-preview "+testRunID)
	assert.Contains(t, env.Error.Message, "release_id")
	assert.False(t, env.Error.Retryable, "resending the stale release id can never succeed")
}

func TestRunRebase_OtherConflictsKeepTheServersMessage(t *testing.T) {
	fake := &fakeRunClient{rebaseErr: status.Error(codes.FailedPrecondition, "the schedule already has an active run")}

	stdout, _, exit := execute(t, NewRebaseCommand, fake, testRunID, "rel-9")

	assert.Equal(t, 4, exit)
	var env struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, "the schedule already has an active run", env.Error.Message)
	assert.True(t, env.Error.Retryable, "an active run may have ended by the next try")
}

// The hint is keyed on the message the run-controller service documents for a moved
// release in run.proto; this fails if the two drift apart.
func TestLiveReleaseMoved_IsTheMessageRunProtoDocuments(t *testing.T) {
	proto, err := os.ReadFile("../../../proto/run/v1/run.proto")
	require.NoError(t, err)

	// The comment wraps the message after "preview", so the test pins the part
	// that precedes the wrap.
	assert.Contains(t, string(proto), `(message "the `+liveReleaseMoved+`; preview`)
}

func TestRunRebase_ErrorsKeepTheirExitCodes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		exit int
	}{
		"not found":          {status.Error(codes.NotFound, "no such run"), 3},
		"request id reused":  {status.Error(codes.AlreadyExists, "request_id reused"), 4},
		"unavailable":        {status.Error(codes.Unavailable, "down"), 5},
		"unexpected failure": {status.Error(codes.Internal, "boom"), 6},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, exit := execute(t, NewRebaseCommand, &fakeRunClient{rebaseErr: tc.err}, testRunID, "rel-9")
			assert.Equal(t, tc.exit, exit)
		})
	}
}

func TestRunRebase_HumanModeWritesToStderrOnly(t *testing.T) {
	fake := &fakeRunClient{rebaseResp: &runv1.TriggerRebaseResponse{RunId: "new-run", ScheduleName: "daily"}}

	stdout, stderr, exit := executeHuman(t, NewRebaseCommand, fake, testRunID, "rel-9")

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "new-run")
}

func TestRunRebase_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeRunClient{rebaseResp: &runv1.TriggerRebaseResponse{RunId: "new-run", ScheduleName: "daily"}}
	cmd := NewRebaseCommand(nil, nil, nil, nil)

	stdout, _, exit := execute(t, NewRebaseCommand, fake, testRunID, "rel-9")

	require.Equal(t, 0, exit)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

func TestRunRebase_LongTellsTheCallerToPreviewAndAskFirst(t *testing.T) {
	long := NewRebaseCommand(nil, nil, nil, nil).Long

	assert.Contains(t, long, "rebase-preview")
	assert.Contains(t, long, "approval")
	assert.Contains(t, long, "release_id")
}
