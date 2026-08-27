package promptlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// fakeProvider records the request it was handed and returns a fixed result or
// error, so a test can assert the decorator passes the request through and
// returns the wrapped provider's answer unchanged.
type fakeProvider struct {
	gotReq ports.ProposeRequest
	calls  int
	result ports.ProposeResult
	err    error
}

func (f *fakeProvider) Propose(_ context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	f.calls++
	f.gotReq = req
	if f.err != nil {
		return ports.ProposeResult{}, f.err
	}
	return f.result, nil
}

// newBufLogger returns a JSON slog logger writing to a buffer, so the test can
// read back what was logged.
func newBufLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})), &buf
}

func sampleRequest() ports.ProposeRequest {
	return ports.ProposeRequest{
		System:   "you are a fixer",
		User:     "Service: service-1\nFile models/table_aa.sql:\n{{ config(...) }}\ndbt compile error:\nexpected token ','",
		ToolName: "propose_fix",
	}
}

// decodeLogLine returns the last JSON log record written to buf.
func decodeLogLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatalf("no log line written")
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v (%q)", err, lines[len(lines)-1])
	}
	return rec
}

// The decorator logs the full prompt — system, user, and forced tool — verbatim,
// so the operator can see exactly what was fed to the model.
func TestPropose_LogsFullPrompt(t *testing.T) {
	fake := &fakeProvider{result: ports.ProposeResult{ProposedContent: "fixed", Model: "m"}}
	logger, buf := newBufLogger()
	req := sampleRequest()

	got, err := New(fake, logger).Propose(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ProposedContent != "fixed" || got.Model != "m" {
		t.Fatalf("result not passed through: %+v", got)
	}
	if fake.calls != 1 {
		t.Fatalf("wrapped provider called %d times, want 1", fake.calls)
	}
	if fake.gotReq.User != req.User || fake.gotReq.System != req.System {
		t.Fatalf("request not passed through unchanged: %+v", fake.gotReq)
	}

	rec := decodeLogLine(t, buf)
	if rec["msg"] != logMessage {
		t.Fatalf("log msg = %v, want %q", rec["msg"], logMessage)
	}
	if rec["system"] != req.System {
		t.Fatalf("logged system = %v, want %q", rec["system"], req.System)
	}
	if rec["user"] != req.User {
		t.Fatalf("logged user = %v, want %q", rec["user"], req.User)
	}
	if rec["tool"] != req.ToolName {
		t.Fatalf("logged tool = %v, want %q", rec["tool"], req.ToolName)
	}
}

// When the driver stamps the failure identity on the context, the prompt log
// carries it, so a logged prompt can be tied to the release/node/attempt it was
// built for.
func TestPropose_LogsFailureCorrelation(t *testing.T) {
	fake := &fakeProvider{}
	logger, buf := newBufLogger()
	ctx := ContextWithFailure(context.Background(), Failure{
		Source: "compile", ReleaseID: "rel-ebbcc5a-138", NodeID: "service-1", Attempt: 1,
	})

	if _, err := New(fake, logger).Propose(ctx, sampleRequest()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := decodeLogLine(t, buf)
	if rec["release"] != "rel-ebbcc5a-138" {
		t.Fatalf("logged release = %v", rec["release"])
	}
	if rec["node"] != "service-1" {
		t.Fatalf("logged node = %v", rec["node"])
	}
	if rec["source"] != "compile" {
		t.Fatalf("logged source = %v", rec["source"])
	}
	if rec["attempt"].(float64) != 1 {
		t.Fatalf("logged attempt = %v", rec["attempt"])
	}
}

// A wrapped-provider error is returned unchanged, and the prompt is still logged
// (the model was still called with it).
func TestPropose_PassesThroughErrorAndStillLogs(t *testing.T) {
	sentinel := errors.New("llm down")
	fake := &fakeProvider{err: sentinel}
	logger, buf := newBufLogger()

	_, err := New(fake, logger).Propose(context.Background(), sampleRequest())
	if !errors.Is(err, sentinel) {
		t.Fatalf("error not passed through: %v", err)
	}
	if rec := decodeLogLine(t, buf); rec["msg"] != logMessage {
		t.Fatalf("prompt was not logged on the error path: %v", rec["msg"])
	}
}
