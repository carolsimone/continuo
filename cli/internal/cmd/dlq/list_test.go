package dlq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/config"
	deadletterv1 "github.com/carolsimone/continuo/cli/proto/deadletter/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestList_PassesPositionalFilters(t *testing.T) {
	f := &fakeDLQ{listResp: &deadletterv1.ListDeadLettersResponse{TotalOpen: 1,
		DeadLetters: []*deadletterv1.DeadLetter{{Id: "a", Source: "consumer", Stream: "s:v1", Status: "open"}}}}
	out, _, exit := run(t, NewListCommand, f, []string{"consumer", "s:v1"}, "")
	require.Equal(t, 0, exit)
	assert.Equal(t, "consumer", f.gotSource)
	assert.Equal(t, "s:v1", f.gotStream)
	var p map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(out), &p))
	assert.Contains(t, string(p["dead_letters"]), `"id":"a"`)
	assert.Equal(t, "1", string(p["total_open"]))
}

func TestList_NoArgsSendsNoFilters(t *testing.T) {
	f := &fakeDLQ{listResp: &deadletterv1.ListDeadLettersResponse{}}
	out, _, exit := run(t, NewListCommand, f, nil, "")
	require.Equal(t, 0, exit)
	assert.Equal(t, "", f.gotSource)
	assert.Equal(t, "", f.gotStream)
	assert.Contains(t, out, `"dead_letters":[]`, "an empty list encodes as [] not null")
}

func TestList_AllSourceSendsNoSourceFilter(t *testing.T) {
	f := &fakeDLQ{listResp: &deadletterv1.ListDeadLettersResponse{}}
	_, _, exit := run(t, NewListCommand, f, []string{"all", "s:v1"}, "")
	require.Equal(t, 0, exit)
	assert.Equal(t, "", f.gotSource)
	assert.Equal(t, "s:v1", f.gotStream)
}

func TestList_BlankOptionalIsUsageError(t *testing.T) {
	for _, args := range [][]string{{" "}, {""}, {"consumer", " "}} {
		_, _, exit := run(t, NewListCommand, &fakeDLQ{}, args, "")
		assert.Equal(t, 2, exit, "args %q", args)
	}
}

func TestList_TooManyArgs(t *testing.T) {
	_, _, exit := run(t, NewListCommand, &fakeDLQ{}, []string{"a", "b", "c"}, "")
	assert.Equal(t, 2, exit)
}

func TestList_InvalidSourceFromServerExits2(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.InvalidArgument, "unknown source")}
	_, _, exit := run(t, NewListCommand, f, []string{"bogus"}, "")
	assert.Equal(t, 2, exit)
}

func TestList_UnavailableExits5(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.Unavailable, "down")}
	_, _, exit := run(t, NewListCommand, f, nil, "")
	assert.Equal(t, 5, exit)
}

func TestList_HumanWritesToStderrOnly(t *testing.T) {
	f := &fakeDLQ{listResp: &deadletterv1.ListDeadLettersResponse{TotalOpen: 1,
		DeadLetters: []*deadletterv1.DeadLetter{{Id: "a", Source: "consumer", FailureKind: "permanent", Stream: "s:v1", ConsumerGroup: "g", Error: "boom"}}}}
	out, errb, exit := runWith(t, NewListCommand, f, nil, &config.Config{Timeout: 2 * time.Second, Human: true})
	require.Equal(t, 0, exit)
	assert.Empty(t, out)
	assert.Contains(t, errb, "boom")
	assert.Contains(t, errb, "1 open")
}

func TestList_OutputSchemaMatchesEmittedKeys(t *testing.T) {
	f := &fakeDLQ{listResp: &deadletterv1.ListDeadLettersResponse{}}
	out, _, _ := run(t, NewListCommand, f, nil, "")
	assertSchemaMatches(t, NewListCommand, out)
}
