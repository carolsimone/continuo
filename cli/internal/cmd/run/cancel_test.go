package run

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func cancellingRun() *runv1.Run {
	r := fullRun()
	r.Status = "cancelling"
	return r
}

func TestRunCancel_IsMutatingAndSendsNoRequestID(t *testing.T) {
	cmd := NewCancelCommand(nil, nil, nil, nil)
	assert.Equal(t, "true", cmd.Annotations["mutating"])

	fake := &fakeRunClient{cancelRun: cancellingRun()}
	t.Setenv("CONTINUO_REQUEST_ID", "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b")

	stdout, stderr, exit := executeAs(t, NewCancelCommand, fake, "agent-chat-llm", testRunID, "bad upstream data")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	assert.Equal(t, []cancelCall{{runID: testRunID, reason: "bad upstream data", by: "agent-chat-llm"}}, fake.cancelCalls,
		"CancelRun takes no request id: the client method has no parameter for one")
	var p struct {
		Run map[string]any `json:"run"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	assert.Equal(t, testRunID, p.Run["run_id"])
	assert.Equal(t, "cancelling", p.Run["status"])
	assert.Equal(t, true, fake.deadlineCalls[0], "the RPC runs under the --timeout deadline")
}

func TestRunCancel_RefusesAMissingOrBlankArgument(t *testing.T) {
	for name, args := range map[string][]string{
		"no arguments":   {},
		"no reason":      {testRunID},
		"blank run id":   {"", "why"},
		"blank reason":   {testRunID, ""},
		"extra argument": {testRunID, "why", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeRunClient{}

			stdout, _, exit := execute(t, NewCancelCommand, fake, args...)

			assert.Equal(t, 2, exit)
			var env map[string]output.CLIError
			require.NoError(t, json.Unmarshal([]byte(stdout), &env))
			assert.Equal(t, output.CodeUsage, env["error"].Code)
			assert.Empty(t, fake.cancelCalls, "no RPC is sent")
		})
	}
}

func TestRunCancel_ErrorsKeepTheirExitCodes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		exit int
	}{
		"not found":          {status.Error(codes.NotFound, "no such run"), 3},
		"already ended":      {status.Error(codes.FailedPrecondition, "run ended succeeded"), 4},
		"unavailable":        {status.Error(codes.Unavailable, "down"), 5},
		"invalid run id":     {status.Error(codes.InvalidArgument, "run_id is not a UUID"), 2},
		"unexpected failure": {status.Error(codes.Internal, "boom"), 6},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, exit := execute(t, NewCancelCommand, &fakeRunClient{cancelErr: tc.err}, testRunID, "why")
			assert.Equal(t, tc.exit, exit)
		})
	}
}

func TestRunCancel_HumanModeWritesToStderrOnly(t *testing.T) {
	fake := &fakeRunClient{cancelRun: cancellingRun()}

	stdout, stderr, exit := executeHuman(t, NewCancelCommand, fake, testRunID, "why")

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, testRunID)
	assert.Contains(t, stderr, "cancelling")
}

func TestRunCancel_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeRunClient{cancelRun: fullRun()}
	cmd := NewCancelCommand(nil, nil, nil, nil)

	stdout, _, exit := execute(t, NewCancelCommand, fake, testRunID, "why")

	require.Equal(t, 0, exit)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

func TestRunCancel_LongSaysTheCancelIsAsynchronousAndRepeatable(t *testing.T) {
	long := NewCancelCommand(nil, nil, nil, nil).Long

	assert.Contains(t, long, "cancelling")
	assert.Contains(t, long, "A second cancel of a cancelling run succeeds and changes nothing")
}
