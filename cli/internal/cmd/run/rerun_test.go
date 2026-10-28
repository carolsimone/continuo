package run

import (
	"encoding/json"
	"testing"

	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testRequestID = "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b"

func TestRunRerun_SendsTheInvocationsRequestID(t *testing.T) {
	t.Setenv("CONTINUO_REQUEST_ID", testRequestID)
	fake := &fakeRunClient{rerunResp: &runv1.TriggerRerunResponse{RunId: "new-run", ScheduleName: "daily"}}

	stdout, stderr, exit := executeAs(t, NewRerunCommand, fake, "alice", testRunID)

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	assert.Equal(t, []rerunCall{{requestID: testRequestID, runID: testRunID, actor: "alice"}}, fake.rerunCalls)
	assert.JSONEq(t, `{"run_id":"new-run","schedule_name":"daily"}`, stdout)
	assert.Equal(t, true, fake.deadlineCalls[0])
}

func TestRunRerun_WithoutTheEnvironmentVariableMintsOneIDPerInvocation(t *testing.T) {
	t.Setenv("CONTINUO_REQUEST_ID", "")
	fake := &fakeRunClient{rerunResp: &runv1.TriggerRerunResponse{RunId: "new-run", ScheduleName: "daily"}}

	_, _, exit := execute(t, NewRerunCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	require.Len(t, fake.rerunCalls, 1)
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, fake.rerunCalls[0].requestID)
}

func TestRunRerun_IsMutating(t *testing.T) {
	assert.Equal(t, "true", NewRerunCommand(nil, nil, nil, nil).Annotations["mutating"])
}

func TestRunRerun_RefusesAMissingBlankOrExtraArgument(t *testing.T) {
	for name, args := range map[string][]string{
		"no arguments":   {},
		"blank run id":   {""},
		"extra argument": {testRunID, "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeRunClient{}

			_, _, exit := execute(t, NewRerunCommand, fake, args...)

			assert.Equal(t, 2, exit)
			assert.Empty(t, fake.rerunCalls)
		})
	}
}

func TestRunRerun_ErrorsKeepTheirExitCodes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		exit int
	}{
		"not found":          {status.Error(codes.NotFound, "no such run"), 3},
		"not rerunnable":     {status.Error(codes.FailedPrecondition, "the run did not fail"), 4},
		"request id reused":  {status.Error(codes.AlreadyExists, "request_id reused"), 4},
		"unavailable":        {status.Error(codes.Unavailable, "down"), 5},
		"unexpected failure": {status.Error(codes.Internal, "boom"), 6},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, exit := execute(t, NewRerunCommand, &fakeRunClient{rerunErr: tc.err}, testRunID)
			assert.Equal(t, tc.exit, exit)
		})
	}
}

func TestRunRerun_HumanModeWritesToStderrOnly(t *testing.T) {
	fake := &fakeRunClient{rerunResp: &runv1.TriggerRerunResponse{RunId: "new-run", ScheduleName: "daily"}}

	stdout, stderr, exit := executeHuman(t, NewRerunCommand, fake, testRunID)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "new-run")
}

func TestRunRerun_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeRunClient{rerunResp: &runv1.TriggerRerunResponse{RunId: "new-run", ScheduleName: "daily"}}
	cmd := NewRerunCommand(nil, nil, nil, nil)

	stdout, _, exit := execute(t, NewRerunCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}
