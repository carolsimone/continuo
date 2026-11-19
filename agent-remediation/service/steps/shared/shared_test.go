package shared_test

import (
	"context"
	"io"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

type fakeEvidence struct {
	body  string
	err   error
	calls []string
}

func (f *fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	f.calls = append(f.calls, uri)
	return f.body, f.err
}

type fakePrecedents struct {
	bySignature map[string][]prompt.Precedent
	byClass     map[string][]prompt.Precedent
	err         error
	queries     []ports.PrecedentQuery
}

func (f *fakePrecedents) Precedents(_ context.Context, q ports.PrecedentQuery) ([]prompt.Precedent, error) {
	f.queries = append(f.queries, q)
	if f.err != nil {
		return nil, f.err
	}
	if q.Signature != "" {
		return f.bySignature[q.Signature], nil
	}
	return f.byClass[q.Category+"/"+q.Reason], nil
}

type fakeUpstream struct {
	changes []prompt.UpstreamChange
	err     error
	nodes   []string
}

func (f *fakeUpstream) UpstreamChanges(_ context.Context, id string) ([]prompt.UpstreamChange, error) {
	f.nodes = append(f.nodes, id)
	return f.changes, f.err
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
