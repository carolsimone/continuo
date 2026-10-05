package dlq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	deadletterv1 "github.com/carolsimone/continuo/cli/proto/deadletter/v1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDLQ struct {
	listResp    *deadletterv1.ListDeadLettersResponse
	getResp     *deadletterv1.GetDeadLetterResponse
	redriveResp *deadletterv1.RedriveDeadLettersResponse
	err         error

	gotSource, gotStream, gotID, gotReason, gotActor string
	gotIDs                                           []string
}

func (f *fakeDLQ) List(_ context.Context, source, stream string) (*deadletterv1.ListDeadLettersResponse, error) {
	f.gotSource, f.gotStream = source, stream
	return f.listResp, f.err
}

func (f *fakeDLQ) Get(_ context.Context, id string) (*deadletterv1.GetDeadLetterResponse, error) {
	f.gotID = id
	return f.getResp, f.err
}

func (f *fakeDLQ) Redrive(_ context.Context, ids []string, reason, actor string) (*deadletterv1.RedriveDeadLettersResponse, error) {
	f.gotIDs, f.gotReason, f.gotActor = ids, reason, actor
	return f.redriveResp, f.err
}

func (f *fakeDLQ) Close() error { return nil }

type builder func(ClientFactory, *config.Config, io.Writer, io.Writer) *cobra.Command

// run executes one dlq subcommand built by build against fake and returns
// stdout, stderr and the exit code.
func run(t *testing.T, build builder, fake client.DeadLetterClient, args []string, actor string) (string, string, int) {
	t.Helper()
	return runWith(t, build, fake, args, &config.Config{Timeout: 2 * time.Second, Actor: actor})
}

func runWith(t *testing.T, build builder, fake client.DeadLetterClient, args []string, cfg *config.Config) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := build(func(context.Context, string) (client.DeadLetterClient, error) { return fake, nil }, cfg, &out, &errb)
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&errb)
	err := cmd.Execute()
	exit := 0
	var cliErr output.CLIError
	if errors.As(err, &cliErr) {
		exit = cliErr.ExitCode()
	} else if err != nil {
		exit = 1
	}
	return out.String(), errb.String(), exit
}

// assertSchemaMatches compares the top-level keys of the command's declared
// output_schema annotation with those of the JSON it emitted.
func assertSchemaMatches(t *testing.T, build builder, stdout string) {
	t.Helper()
	var emitted map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))
	cmd := build(nil, &config.Config{}, io.Discard, io.Discard)
	var schema map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	assert.ElementsMatch(t, keys(emitted), keys(schema))
}

func keys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
