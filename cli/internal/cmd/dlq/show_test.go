package dlq

import (
	"encoding/json"
	"testing"

	deadletterv1 "github.com/carolsimone/continuo/cli/proto/deadletter/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func sampleShowResp() *deadletterv1.GetDeadLetterResponse {
	return &deadletterv1.GetDeadLetterResponse{
		DeadLetter: &deadletterv1.DeadLetter{Id: "a", Source: "outbox", Status: "open", Redrivable: true,
			Redrive: &deadletterv1.Redrive{Actor: "alice", Reason: "r", At: "2026-10-01T00:00:00Z"}},
		Fields:            map[string]string{"k": "v"},
		OriginalEventType: "node.updated",
		FailedOutboxId:    "42",
	}
}

func TestShow_PrintsDeadLetterAndFields(t *testing.T) {
	f := &fakeDLQ{getResp: sampleShowResp()}
	out, _, exit := run(t, NewShowCommand, f, []string{"a"}, "")
	require.Equal(t, 0, exit)
	assert.Equal(t, "a", f.gotID)
	var p struct {
		DeadLetter        deadLetterJSON    `json:"dead_letter"`
		Fields            map[string]string `json:"fields"`
		OriginalEventType string            `json:"original_event_type"`
		FailedOutboxID    string            `json:"failed_outbox_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &p))
	assert.Equal(t, "a", p.DeadLetter.ID)
	assert.Equal(t, "alice", p.DeadLetter.Redrive.Actor)
	assert.Equal(t, map[string]string{"k": "v"}, p.Fields)
	assert.Equal(t, "node.updated", p.OriginalEventType)
	assert.Equal(t, "42", p.FailedOutboxID)
}

func TestShow_NoFieldsEncodesEmptyObject(t *testing.T) {
	f := &fakeDLQ{getResp: &deadletterv1.GetDeadLetterResponse{DeadLetter: &deadletterv1.DeadLetter{Id: "a"}}}
	out, _, exit := run(t, NewShowCommand, f, []string{"a"}, "")
	require.Equal(t, 0, exit)
	assert.Contains(t, out, `"fields":{}`)
	assert.Contains(t, out, `"redrive":null`)
}

func TestShow_NotFoundExits3(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.NotFound, "no such dead letter")}
	out, _, exit := run(t, NewShowCommand, f, []string{"a"}, "")
	assert.Equal(t, 3, exit)
	assert.Contains(t, out, `"not_found"`)
}

func TestShow_InvalidIDFromServerExits2(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.InvalidArgument, "bad id")}
	_, _, exit := run(t, NewShowCommand, f, []string{"zzz"}, "")
	assert.Equal(t, 2, exit)
}

func TestShow_MissingOrBlankIDIsUsage(t *testing.T) {
	for _, args := range [][]string{nil, {" "}, {"a", "b"}} {
		_, _, exit := run(t, NewShowCommand, &fakeDLQ{}, args, "")
		assert.Equal(t, 2, exit, "args %q", args)
	}
}

func TestShow_OutputSchemaMatchesEmittedKeys(t *testing.T) {
	f := &fakeDLQ{getResp: sampleShowResp()}
	out, _, _ := run(t, NewShowCommand, f, []string{"a"}, "")
	assertSchemaMatches(t, NewShowCommand, out)
}
