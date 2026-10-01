package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeNode struct {
	schedule, service, imageTag string
}

func fakeAccessor(n fakeNode) (string, string, string) {
	return n.schedule, n.service, n.imageTag
}

func TestScheduleAndMetadataFromNodes_DedupesSortsAndFirstWins(t *testing.T) {
	nodes := []fakeNode{
		{"daily", "svc-a", "sha:1"},
		{"hourly", "svc-b", "sha:2"},
		{"daily", "svc-a", "sha:9"}, // duplicate svc-a — first wins; second image_tag DIFFERS
		{"", "svc-c", "sha:3"},      // empty schedule must be filtered
	}

	schedules, metadata := scheduleAndMetadataFromNodes(nodes, fakeAccessor)

	assert.Equal(t, []string{"daily", "hourly"}, schedules)
	assert.Equal(t, map[string]map[string]string{
		"svc-a": {"image_tag": "sha:1"},
		"svc-b": {"image_tag": "sha:2"},
		"svc-c": {"image_tag": "sha:3"},
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
		{"weekly", "svc-x", "sha:abc"},
	}
	schedules, metadata := scheduleAndMetadataFromNodes(nodes, fakeAccessor)
	assert.Equal(t, []string{"weekly"}, schedules)
	assert.Equal(t, map[string]map[string]string{
		"svc-x": {"image_tag": "sha:abc"},
	}, metadata)
}

func TestScheduleAndMetadataFromNodes_MultipleServicesAllSchedulesSorted(t *testing.T) {
	// Verifies output is stable regardless of map iteration order.
	nodes := []fakeNode{
		{"zebra", "svc-z", "t1"},
		{"alpha", "svc-a", "t2"},
		{"mango", "svc-m", "t3"},
	}
	schedules, metadata := scheduleAndMetadataFromNodes(nodes, fakeAccessor)
	assert.Equal(t, []string{"alpha", "mango", "zebra"}, schedules)
	assert.Len(t, metadata, 3)
}
