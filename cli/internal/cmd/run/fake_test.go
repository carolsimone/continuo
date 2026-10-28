package run

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
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testRunID = "11111111-1111-4111-8111-111111111111"

// fakeRunClient implements the read RPCs of client.RunClient; any other method
// panics through the nil embedded interface, which a test never reaches.
type fakeRunClient struct {
	client.RunClient

	run        *runv1.Run
	getRunErr  error
	taskPages  []*runv1.ListTasksResponse
	attemptPgs []*runv1.ListRunAttemptsResponse
	graph      *runv1.GetRunGraphResponse
	graphErr   error

	getRunCalls   []string
	attemptCalls  []attemptCall
	graphCalls    int
	deadlineCalls []bool
}

type attemptCall struct {
	runID, taskID string
	pageSize      int32
	pageOffset    int32
}

func (f *fakeRunClient) GetRun(ctx context.Context, runID string) (*runv1.RunResponse, error) {
	_, has := ctx.Deadline()
	f.deadlineCalls = append(f.deadlineCalls, has)
	f.getRunCalls = append(f.getRunCalls, runID)
	if f.getRunErr != nil {
		return nil, f.getRunErr
	}
	return &runv1.RunResponse{Run: f.run}, nil
}

func (f *fakeRunClient) ListTasks(ctx context.Context, _, _ string, _, offset int32) (*runv1.ListTasksResponse, error) {
	_, has := ctx.Deadline()
	f.deadlineCalls = append(f.deadlineCalls, has)
	idx := int(offset) / 200
	if idx >= len(f.taskPages) {
		return &runv1.ListTasksResponse{}, nil
	}
	return f.taskPages[idx], nil
}

func (f *fakeRunClient) ListRunAttempts(ctx context.Context, runID, taskID string, pageSize, pageOffset int32) (*runv1.ListRunAttemptsResponse, error) {
	_, has := ctx.Deadline()
	f.deadlineCalls = append(f.deadlineCalls, has)
	f.attemptCalls = append(f.attemptCalls, attemptCall{runID, taskID, pageSize, pageOffset})
	idx := int(pageOffset) / 200
	if idx >= len(f.attemptPgs) {
		return &runv1.ListRunAttemptsResponse{}, nil
	}
	return f.attemptPgs[idx], nil
}

func (f *fakeRunClient) GetRunGraph(ctx context.Context, _ string) (*runv1.GetRunGraphResponse, error) {
	_, has := ctx.Deadline()
	f.deadlineCalls = append(f.deadlineCalls, has)
	f.graphCalls++
	return f.graph, f.graphErr
}

func (f *fakeRunClient) Close() error { return nil }

type commandBuilder func(RunClientFactory, *config.Config, io.Writer, io.Writer) *cobra.Command

// execute runs a command with args and returns its stdout, stderr and exit code.
func execute(t *testing.T, build commandBuilder, fake client.RunClient, args ...string) (stdout, stderr string, exit int) {
	t.Helper()
	return executeMode(t, build, fake, false, args...)
}

// executeHuman is execute with --human set.
func executeHuman(t *testing.T, build commandBuilder, fake client.RunClient, args ...string) (stdout, stderr string, exit int) {
	t.Helper()
	return executeMode(t, build, fake, true, args...)
}

func executeMode(t *testing.T, build commandBuilder, fake client.RunClient, human bool, args ...string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := build(func(context.Context, string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	if err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			exit = cliErr.ExitCode()
		} else {
			exit = 1
		}
	}
	return outBuf.String(), errBuf.String(), exit
}

// requireShapeMatches fails unless emitted has exactly the keys schema
// declares, at every depth. An array is compared through its first element; an
// empty array or a null is not descended into.
func requireShapeMatches(t *testing.T, schema, emitted json.RawMessage) {
	t.Helper()
	var s, e any
	require.NoError(t, json.Unmarshal(schema, &s))
	require.NoError(t, json.Unmarshal(emitted, &e))
	compareShape(t, "$", s, e)
}

func compareShape(t *testing.T, path string, schema, emitted any) {
	t.Helper()
	switch sv := schema.(type) {
	case map[string]any:
		ev, ok := emitted.(map[string]any)
		if !ok {
			assert.Nil(t, emitted, "%s: schema declares an object, output is %T", path, emitted)
			return
		}
		assert.ElementsMatch(t, mapKeys(sv), mapKeys(ev), "%s: keys differ", path)
		for k, child := range sv {
			if ec, ok := ev[k]; ok {
				compareShape(t, path+"."+k, child, ec)
			}
		}
	case []any:
		ev, ok := emitted.([]any)
		require.True(t, ok, "%s: schema declares an array, output is %T", path, emitted)
		if len(sv) > 0 && len(ev) > 0 {
			compareShape(t, path+"[]", sv[0], ev[0])
		}
	}
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func ts(year int, month time.Month, day, hour, minute int) *timestamppb.Timestamp {
	return timestamppb.New(time.Date(year, month, day, hour, minute, 0, 0, time.UTC))
}
