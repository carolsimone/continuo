package main

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
)

func TestTrimTargets_SkipsDeadLetterStreams(t *testing.T) {
	got := trimTargets(streams.All)
	assert.NotContains(t, got, streams.OutboxDeadLetterV1)
	assert.NotContains(t, got, streams.ConsumerDeadLetterV1)
	assert.Contains(t, got, streams.TaskStatusUpdatedV1)
	assert.Len(t, got, len(streams.All)-2)
}
