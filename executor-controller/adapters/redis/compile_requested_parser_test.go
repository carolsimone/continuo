// executor-controller/adapters/redis/compile_requested_parser_test.go
package redis

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compileRequestedPayload builds a valid compile.requested:v1 payload body.
func compileRequestedPayload() map[string]any {
	return map[string]any{
		"release_id": "rel-compile-789",
		"service":    "finance",
		"image_tag":  "sha-compile-abc",
		"bucket":     "my-artifacts-bucket",
	}
}

// compileRequestedMsg builds a goredis.XMessage whose "payload" field holds the
// marshaled JSON body.
func compileRequestedMsg(t *testing.T, payload map[string]any, outboxEntryID string) goredis.XMessage {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	values := map[string]interface{}{"payload": string(body)}
	if outboxEntryID != "" {
		values["outbox_entry_id"] = outboxEntryID
	}
	return goredis.XMessage{ID: "5-0", Values: values}
}

func TestParseCompileRequested_HappyPath(t *testing.T) {
	outboxEntryID := uuid.New()
	msg := compileRequestedMsg(t, compileRequestedPayload(), outboxEntryID.String())

	evt, err := ParseCompileRequested(msg)
	require.NoError(t, err)

	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, "rel-compile-789", evt.ReleaseID)
	assert.Equal(t, "finance", evt.Service)
	assert.Equal(t, "sha-compile-abc", evt.ImageTag)
	assert.Equal(t, "my-artifacts-bucket", evt.Bucket)
}

func TestParseCompileRequested_OutboxEntryIDAbsentIsNilUUID(t *testing.T) {
	msg := compileRequestedMsg(t, compileRequestedPayload(), "")
	evt, err := ParseCompileRequested(msg)
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID)
}

func TestParseCompileRequested_OutboxEntryIDInvalidIsError(t *testing.T) {
	msg := compileRequestedMsg(t, compileRequestedPayload(), "not-a-uuid")
	_, err := ParseCompileRequested(msg)
	require.Error(t, err)
}

func TestParseCompileRequested_MissingPayloadIsError(t *testing.T) {
	_, err := ParseCompileRequested(goredis.XMessage{ID: "5-0", Values: map[string]interface{}{}})
	require.Error(t, err)
}

func TestParseCompileRequested_MalformedPayloadIsError(t *testing.T) {
	_, err := ParseCompileRequested(goredis.XMessage{ID: "5-0", Values: map[string]interface{}{
		"payload": "{not json",
	}})
	require.Error(t, err)
}

func TestParseCompileRequested_MissingReleaseID(t *testing.T) {
	p := compileRequestedPayload()
	delete(p, "release_id")
	_, err := ParseCompileRequested(compileRequestedMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseCompileRequested_MissingService(t *testing.T) {
	p := compileRequestedPayload()
	delete(p, "service")
	_, err := ParseCompileRequested(compileRequestedMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseCompileRequested_MissingImageTag(t *testing.T) {
	p := compileRequestedPayload()
	delete(p, "image_tag")
	_, err := ParseCompileRequested(compileRequestedMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseCompileRequested_MissingBucket(t *testing.T) {
	p := compileRequestedPayload()
	delete(p, "bucket")
	_, err := ParseCompileRequested(compileRequestedMsg(t, p, ""))
	require.Error(t, err)
}
