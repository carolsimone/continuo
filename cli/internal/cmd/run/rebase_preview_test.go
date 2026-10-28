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

func previewResponse() *runv1.PreviewRebaseResponse {
	return &runv1.PreviewRebaseResponse{
		ReleaseId: "rel-9", PromotionSeq: 9, TotalNodes: 12, WillRun: 5, FailedOrSkipped: 2,
		NewNodes: 1, ChangedUpstream: 1, Downstream: 1, Inherited: 7,
		ChangedNodes: []*runv1.ChangedUpstreamNode{
			{ServiceName: "svc", SchemaName: "raw", TableName: "orders", Downstream: 3, LastSuccessSeconds: 41.5},
		},
	}
}

func TestRunRebasePreview_OutputCarriesTheReleaseID(t *testing.T) {
	fake := &fakeRunClient{preview: previewResponse()}

	stdout, stderr, exit := execute(t, NewRebasePreviewCommand, fake, testRunID, "true")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	assert.JSONEq(t, `{
		"release_id":"rel-9","promotion_seq":9,"total_nodes":12,"will_run":5,"failed_or_skipped":2,
		"new_nodes":1,"changed_upstream":1,"downstream":1,"inherited":7,
		"changed_nodes":[{"service_name":"svc","schema_name":"raw","table_name":"orders","downstream":3,"last_success_seconds":41.5}]
	}`, stdout)
	assert.Equal(t, []previewCall{{runID: testRunID, changedUpstream: true}}, fake.previewCalls)
	assert.Equal(t, true, fake.deadlineCalls[0])
}

func TestRunRebasePreview_WithoutTheOptionIncludesNoChangedUpstream(t *testing.T) {
	fake := &fakeRunClient{preview: &runv1.PreviewRebaseResponse{ReleaseId: "rel-9"}}

	stdout, _, exit := execute(t, NewRebasePreviewCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	assert.Equal(t, []previewCall{{runID: testRunID, changedUpstream: false}}, fake.previewCalls)
	assert.Contains(t, stdout, `"changed_nodes":[]`, "an empty list prints as [], not null")
}

func TestRunRebasePreview_IsNotMutating(t *testing.T) {
	assert.NotContains(t, NewRebasePreviewCommand(nil, nil, nil, nil).Annotations, "mutating")
}

func TestRunRebasePreview_RefusesAMissingOrBlankRunID(t *testing.T) {
	for name, args := range map[string][]string{
		"no arguments":   {},
		"blank run id":   {""},
		"extra argument": {testRunID, "true", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeRunClient{}

			_, _, exit := execute(t, NewRebasePreviewCommand, fake, args...)

			assert.Equal(t, 2, exit)
			assert.Empty(t, fake.previewCalls)
		})
	}
}

func TestRunRebasePreview_ErrorsKeepTheirExitCodes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		exit int
	}{
		"not found":          {status.Error(codes.NotFound, "no such run"), 3},
		"not rebasable":      {status.Error(codes.FailedPrecondition, "the run did not fail"), 4},
		"unavailable":        {status.Error(codes.Unavailable, "down"), 5},
		"unexpected failure": {status.Error(codes.Internal, "boom"), 6},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, exit := execute(t, NewRebasePreviewCommand, &fakeRunClient{previewErr: tc.err}, testRunID)
			assert.Equal(t, tc.exit, exit)
		})
	}
}

func TestRunRebasePreview_HumanModeNamesTheReleaseAndCounts(t *testing.T) {
	fake := &fakeRunClient{preview: previewResponse()}

	stdout, stderr, exit := executeHuman(t, NewRebasePreviewCommand, fake, testRunID)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "rel-9")
	assert.Contains(t, stderr, "svc.raw.orders")
}

func TestRunRebasePreview_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeRunClient{preview: previewResponse()}
	cmd := NewRebasePreviewCommand(nil, nil, nil, nil)

	stdout, _, exit := execute(t, NewRebasePreviewCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

// Both rebase commands take the same bracketed boolean, and anything but the
// two spellings is a usage error before any RPC is sent.
func TestWithChangedUpstream_RefusesAnythingButTrueOrFalse(t *testing.T) {
	for _, bad := range []string{"", "yes", "1", "True", "TRUE", "no", " true", "0"} {
		t.Run("preview "+bad, func(t *testing.T) {
			fake := &fakeRunClient{}

			stdout, _, exit := execute(t, NewRebasePreviewCommand, fake, testRunID, bad)

			assert.Equal(t, 2, exit)
			var env map[string]output.CLIError
			require.NoError(t, json.Unmarshal([]byte(stdout), &env))
			assert.Equal(t, output.CodeUsage, env["error"].Code)
			assert.Empty(t, fake.previewCalls)
		})
		t.Run("rebase "+bad, func(t *testing.T) {
			fake := &fakeRunClient{}

			_, _, exit := execute(t, NewRebaseCommand, fake, testRunID, "rel-9", bad)

			assert.Equal(t, 2, exit)
			assert.Empty(t, fake.rebaseCalls)
		})
	}

	for value, want := range map[string]bool{"true": true, "false": false} {
		fake := &fakeRunClient{preview: &runv1.PreviewRebaseResponse{}}
		_, _, exit := execute(t, NewRebasePreviewCommand, fake, testRunID, value)
		require.Equal(t, 0, exit)
		assert.Equal(t, want, fake.previewCalls[0].changedUpstream)
	}
}
