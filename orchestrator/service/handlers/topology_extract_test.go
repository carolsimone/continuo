package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeNode struct {
	schedule, service, imageTag, manifestVersion string
}

func fakeAccessor(n fakeNode) (string, string, string, string) {
	return n.schedule, n.service, n.imageTag, n.manifestVersion
}

func TestScheduleAndMetadataFromNodes_DedupesSortsAndFirstWins(t *testing.T) {
	nodes := []fakeNode{
		{"daily", "svc-a", "sha:1", "v1"},
		{"hourly", "svc-b", "sha:2", "v1"},
		{"daily", "svc-a", "sha:9", "v1"}, // duplicate svc-a — first wins; second image_tag DIFFERS
		{"", "svc-c", "sha:3", "v1"},      // empty schedule must be filtered
	}

	schedules, metadata := scheduleAndMetadataFromNodes(nodes, fakeAccessor)

	assert.Equal(t, []string{"daily", "hourly"}, schedules)
	assert.Equal(t, map[string]map[string]string{
		"svc-a": {"manifest_version": "v1", "image_tag": "sha:1"},
		"svc-b": {"manifest_version": "v1", "image_tag": "sha:2"},
		"svc-c": {"manifest_version": "v1", "image_tag": "sha:3"},
	}, metadata)
}

func TestScheduleAndMetadataFromNodes_Empty(t *testing.T) {
	schedules, metadata := scheduleAndMetadataFromNodes(
		[]fakeNode{},
		fakeAccessor,
	)
	assert.Empty(t, schedules)
	assert.Empty(t, metadata)
}

func TestScheduleAndMetadataFromNodes_SingleNode(t *testing.T) {
	nodes := []fakeNode{
		{"weekly", "svc-x", "sha:abc", "v42"},
	}
	schedules, metadata := scheduleAndMetadataFromNodes(nodes, fakeAccessor)
	assert.Equal(t, []string{"weekly"}, schedules)
	assert.Equal(t, map[string]map[string]string{
		"svc-x": {"manifest_version": "v42", "image_tag": "sha:abc"},
	}, metadata)
}

func TestScheduleAndMetadataFromNodes_MultipleServicesAllSchedulesSorted(t *testing.T) {
	// Verifies output is stable regardless of map iteration order.
	nodes := []fakeNode{
		{"zebra", "svc-z", "t1", "v1"},
		{"alpha", "svc-a", "t2", "v2"},
		{"mango", "svc-m", "t3", "v3"},
	}
	schedules, metadata := scheduleAndMetadataFromNodes(nodes, fakeAccessor)
	assert.Equal(t, []string{"alpha", "mango", "zebra"}, schedules)
	assert.Len(t, metadata, 3)
}
