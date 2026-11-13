package source_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// markSanitizer wraps what it is given, so a test can tell sanitized text from
// raw text.
type markSanitizer struct{}

func (markSanitizer) Sanitize(s string) string { return "<" + s + ">" }

var _ ports.LogSanitizer = markSanitizer{}

// fakeSource serves files and directory listings from maps and records reads.
type fakeSource struct {
	files   map[string]string
	readErr map[string]error
	dirErr  error
	reads   []string
}

func (f *fakeSource) ReadFile(_ context.Context, _, _, p string) (string, error) {
	f.reads = append(f.reads, p)
	if err := f.readErr[p]; err != nil {
		return "", err
	}
	c, ok := f.files[p]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

func (f *fakeSource) ListDir(_ context.Context, _, _, dir string) ([]string, error) {
	if f.dirErr != nil {
		return nil, f.dirErr
	}
	var out []string
	for p := range f.files {
		if path.Dir(p) == dir {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}

var _ ports.SourceReader = (*fakeSource)(nil)

type fakeLocator struct {
	filePath, service string
	err               error
	calls             []string
}

func (f *fakeLocator) Locate(_ context.Context, id string) (string, string, error) {
	f.calls = append(f.calls, id)
	return f.filePath, f.service, f.err
}

var _ ports.NodeLocator = (*fakeLocator)(nil)

// fakeModel records every request and answers with the scripted results in
// order.
type fakeModel struct {
	results []ports.ProposeResult
	err     error
	reqs    []prompt.ProposeRequest
}

func (f *fakeModel) Propose(_ context.Context, req prompt.ProposeRequest) (ports.ProposeResult, error) {
	f.reqs = append(f.reqs, req)
	if f.err != nil {
		return ports.ProposeResult{}, f.err
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r, nil
}

var errBoom = errors.New("boom")

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
