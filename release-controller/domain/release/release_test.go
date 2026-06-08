package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelease_NewIsReceived(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, time.Unix(0, 0))
	assert.Equal(t, release.StatusReceived, r.Status())
}

func TestRelease_NewInitialisesImageTags(t *testing.T) {
	r := release.New("sha-abc", "svc1", "sha-abc", false, time.Unix(0, 0))
	assert.Equal(t, map[string]string{"svc1": "sha-abc"}, r.ImageTags())
	assert.Equal(t, "svc1", r.ChangedService())
}

func TestRelease_TransitionReceivedToParsing(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	assert.Equal(t, release.StatusParsing, r.Status())
}

func TestRelease_TransitionParsingToValidating(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	assert.Equal(t, release.StatusValidating, r.Status())
	assert.Equal(t, []string{"n"}, r.ValidationNodeIDs())
}

func TestRelease_TransitionValidatingToPromoted(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Equal(t, release.StatusPromoted, r.Status())
}

func TestRelease_TransitionValidatingToRejected(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"n"}, time.Unix(3, 0)))
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
}

func TestRelease_CannotSkipStates(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	assert.Error(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(1, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(1, 0)))
}

func TestRelease_CannotDoublePromote(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(4, 0)))
}

func TestRelease_TransitionsAreRecorded(t *testing.T) {
	r := release.New("sha-abc", "svc", "tag", false, time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	transitions := r.Transitions()
	require.Len(t, transitions, 3)
	assert.Equal(t, release.StatusReceived, transitions[0].To)
	assert.Equal(t, release.StatusParsing, transitions[1].To)
	assert.Equal(t, release.StatusValidating, transitions[2].To)
}

func TestRelease_SetAssembledImageTags(t *testing.T) {
	r := release.New("sha-abc", "svc-a", "tag-a", false, time.Unix(0, 0))
	assembled := map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}
	r.SetAssembledImageTags(assembled)
	assert.Equal(t, assembled, r.ImageTags())
}

func TestRecordValidationResults_RetainedAcrossReject(t *testing.T) {
	r := release.New("rX", "svc", "t", false, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))

	results := []release.NodeValidationResult{
		{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 12},
	}
	r.RecordValidationResults(results)
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"a"}, time.Unix(4, 0).UTC()))

	assert.Equal(t, results, r.PerNodeResults())
	assert.Equal(t, release.StatusRejected, r.Status())
}

func TestRecordValidationResults_RetainedAcrossPromote(t *testing.T) {
	r := release.New("rY", "svc", "t", false, time.Unix(1, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(2, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.Topology{{UniqueID: "a"}}, []string{"a"}, time.Unix(3, 0).UTC()))
	r.RecordValidationResults([]release.NodeValidationResult{{NodeID: "a", Status: "ok"}})
	require.NoError(t, r.TransitionToPromoted(time.Unix(4, 0).UTC()))
	assert.Equal(t, []release.NodeValidationResult{{NodeID: "a", Status: "ok"}}, r.PerNodeResults())
}
