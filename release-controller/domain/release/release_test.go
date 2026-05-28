package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelease_NewIsReceived(t *testing.T) {
	r := release.New("sha-abc", []string{"svc1.t_a"}, map[string]string{"svc1": "sha-abc"}, "s3://b/r/sha-abc/manifests/", time.Unix(0, 0))
	assert.Equal(t, release.StatusReceived, r.Status())
}

func TestRelease_TransitionReceivedToParsing(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	assert.Equal(t, release.StatusParsing, r.Status())
}

func TestRelease_TransitionParsingToValidating(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	assert.Equal(t, release.StatusValidating, r.Status())
	assert.Equal(t, []string{"n"}, r.ValidationNodeIDs())
}

func TestRelease_TransitionValidatingToPromoted(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Equal(t, release.StatusPromoted, r.Status())
}

func TestRelease_TransitionValidatingToRejected(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"n"}, "s3://logs", time.Unix(3, 0)))
	assert.Equal(t, release.StatusRejected, r.Status())
	assert.Equal(t, "validation_failed", r.RejectReason())
}

func TestRelease_CannotSkipStates(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	assert.Error(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(1, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(1, 0)))
}

func TestRelease_CannotDoublePromote(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	require.NoError(t, r.TransitionToPromoted(time.Unix(3, 0)))
	assert.Error(t, r.TransitionToPromoted(time.Unix(4, 0)))
}

func TestRelease_TransitionsAreRecorded(t *testing.T) {
	r := release.New("sha-abc", []string{"n"}, nil, "u", time.Unix(0, 0))
	require.NoError(t, r.TransitionToParsing(time.Unix(1, 0)))
	require.NoError(t, r.TransitionToValidating(release.Topology{}, []string{"n"}, time.Unix(2, 0)))
	transitions := r.Transitions()
	require.Len(t, transitions, 3)
	assert.Equal(t, release.StatusReceived, transitions[0].To)
	assert.Equal(t, release.StatusParsing, transitions[1].To)
	assert.Equal(t, release.StatusValidating, transitions[2].To)
}
