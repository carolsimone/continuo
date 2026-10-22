package events_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Record carries every fact of the event onto the aggregate's record.
func TestTaskExecutionRecorded_RecordMapsEveryField(t *testing.T) {
	taskID := uuid.New()
	started := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	completed := started.Add(time.Minute)
	secs := 60.0
	msg, logKey, results, cache, reason := "boom", "logs/j.log", "run-results/j.json", "miss", "manifest changed"
	evt := events.TaskExecutionRecorded{
		ExecutionID: uuid.New(), TaskID: taskID, JobName: "job-r2",
		StartedAt: &started, CompletedAt: &completed, ExecutionTimeSeconds: &secs,
		ErrorMessage: &msg, LogS3Key: &logKey, RunResultsURI: &results,
		ParseCache: &cache, ParseCacheReason: &reason,
	}

	assert.Equal(t, run.ExecutionRecord{
		TaskID: taskID, JobName: "job-r2",
		StartedAt: &started, CompletedAt: &completed, ExecutionTimeSeconds: &secs,
		ErrorMessage: &msg, LogS3Key: &logKey, RunResultsURI: &results,
		ParseCache: &cache, ParseCacheReason: &reason,
	}, evt.Record())
}

// The event and the record name the same facts: every field of the record is
// a field of the event, and every field of the event but its execution id is
// a field of the record, with the same type. A fact added to one and not the
// other fails here.
func TestTaskExecutionRecorded_RecordAndEventNameTheSameFacts(t *testing.T) {
	evtType := reflect.TypeFor[events.TaskExecutionRecorded]()
	recType := reflect.TypeFor[run.ExecutionRecord]()

	for f := range recType.Fields() {
		ef, ok := evtType.FieldByName(f.Name)
		require.True(t, ok, "record field %s is not on the event", f.Name)
		assert.Equal(t, f.Type, ef.Type, "field %s", f.Name)
	}
	for f := range evtType.Fields() {
		if f.Name == "ExecutionID" {
			continue
		}
		_, ok := recType.FieldByName(f.Name)
		assert.True(t, ok, "event field %s is not on the record", f.Name)
	}
}
