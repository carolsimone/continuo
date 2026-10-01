package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postCandidate(srv *Server, in handlers.ReceiveCandidateInput) *httptest.ResponseRecorder {
	body, _ := json.Marshal(in)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body)))
	return rec
}

func validCandidateInput() handlers.ReceiveCandidateInput {
	return handlers.ReceiveCandidateInput{Service: "core", ReleaseID: "rel-1", ImageTag: "img:1", Repo: "org/r", CommitSHA: "sha"}
}

func TestHandleReceiveCandidate_Accepted(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())

	rec := postCandidate(newTestServer(deps), validCandidateInput())

	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.NotNil(t, releases.releases["rel-1"])
}

// A submission the caller got wrong is a client error naming the field.
func TestHandleReceiveCandidate_ValidationFailureIs400(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*handlers.ReceiveCandidateInput)
		want   string
	}{
		"missing image_tag": {func(in *handlers.ReceiveCandidateInput) { in.ImageTag = "" }, "image_tag is required"},
		"missing service":   {func(in *handlers.ReceiveCandidateInput) { in.Service = "" }, "service is required"},
		"unknown kind":      {func(in *handlers.ReceiveCandidateInput) { in.Kind = "yaml" }, ""},
	} {
		t.Run(name, func(t *testing.T) {
			deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
			in := validCandidateInput()
			tc.mutate(&in)

			rec := postCandidate(newTestServer(deps), in)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.want)
			assert.Empty(t, releases.releases)
		})
	}
}

// beginFailsUoW stands in for a database that cannot open a transaction; its
// error text carries an internal address that must never reach a caller.
type beginFailsUoW struct{ *fakeUoW }

func (beginFailsUoW) Begin(context.Context) error {
	return errors.New("dial tcp 10.1.2.3:5432: connection refused")
}

// A storage failure is the server's fault: the caller gets a generic 500 and
// the cause is logged, never returned.
func TestHandleReceiveCandidate_StorageFailureIs500WithoutInternalText(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	failing := beginFailsUoW{&fakeUoW{releases: releases, outbox: &fakeOutboxRepo{}}}
	deps.NewUoW = func() uow.UnitOfWork { return failing }
	var logged bytes.Buffer
	srv := NewServer(deps, liveness.NewRegistry(), "0", slog.New(slog.NewTextHandler(&logged, nil)))

	rec := postCandidate(srv, validCandidateInput())

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "internal error\n", rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "10.1.2.3")
	assert.Contains(t, logged.String(), "connection refused")
}
