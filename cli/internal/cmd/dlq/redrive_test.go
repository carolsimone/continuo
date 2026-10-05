package dlq

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	deadletterv1 "github.com/carolsimone/continuo/cli/proto/deadletter/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRedrive_SplitsIDsAndForwardsActor(t *testing.T) {
	f := &fakeDLQ{redriveResp: &deadletterv1.RedriveDeadLettersResponse{DeadLetters: []*deadletterv1.DeadLetter{{Id: "a"}, {Id: "b"}}}}
	out, _, exit := run(t, NewRedriveCommand, f, []string{"a, b,", "fixed upstream"}, "alice")
	require.Equal(t, 0, exit)
	assert.Equal(t, []string{"a", "b"}, f.gotIDs)
	assert.Equal(t, "fixed upstream", f.gotReason)
	assert.Equal(t, "alice", f.gotActor)
	var p map[string][]deadLetterJSON
	require.NoError(t, json.Unmarshal([]byte(out), &p))
	assert.Len(t, p["dead_letters"], 2)
}

func TestRedrive_ConflictExits4(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.FailedPrecondition, "dead letter is past the replay horizon")}
	out, _, exit := run(t, NewRedriveCommand, f, []string{"a", "r"}, "")
	assert.Equal(t, 4, exit)
	assert.Contains(t, out, `"conflict"`)
}

func TestRedrive_NotFoundExits3(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.NotFound, "no such dead letter")}
	_, _, exit := run(t, NewRedriveCommand, f, []string{"a", "r"}, "")
	assert.Equal(t, 3, exit)
}

func TestRedrive_InvalidArgumentFromServerExits2(t *testing.T) {
	f := &fakeDLQ{err: status.Error(codes.InvalidArgument, "bad id")}
	_, _, exit := run(t, NewRedriveCommand, f, []string{"zzz", "r"}, "")
	assert.Equal(t, 2, exit)
}

func TestRedrive_BlankReasonOrIDsIsUsage(t *testing.T) {
	for _, args := range [][]string{{"a", " "}, {" , ", "r"}, {"a"}, {"a", "r", "x"}} {
		f := &fakeDLQ{}
		_, _, exit := run(t, NewRedriveCommand, f, args, "")
		assert.Equal(t, 2, exit, "args %q", args)
		assert.Nil(t, f.gotIDs, "no RPC may be sent for args %q", args)
	}
}

func TestRedrive_IsMutating(t *testing.T) {
	cmd := NewRedriveCommand(nil, &config.Config{}, io.Discard, io.Discard)
	assert.Equal(t, "true", cmd.Annotations["mutating"])
}

func TestRedrive_OutputSchemaMatchesEmittedKeys(t *testing.T) {
	f := &fakeDLQ{redriveResp: &deadletterv1.RedriveDeadLettersResponse{}}
	out, _, _ := run(t, NewRedriveCommand, f, []string{"a", "r"}, "")
	assertSchemaMatches(t, NewRedriveCommand, out)
}
