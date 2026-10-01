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
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postCandidate(srv *Server, in ReceiveCandidateRequest) *httptest.ResponseRecorder {
	body, _ := json.Marshal(in)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body)))
	return rec
}

func validCandidateInput() ReceiveCandidateRequest {
	return ReceiveCandidateRequest{Service: "core", ReleaseID: "rel-1", ImageTag: "img:1", Repo: "org/r", CommitSHA: "sha"}
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
		mutate func(*ReceiveCandidateRequest)
		want   string
	}{
		"missing image_tag": {func(in *ReceiveCandidateRequest) { in.ImageTag = "" }, "image_tag is required"},
		"missing service":   {func(in *ReceiveCandidateRequest) { in.Service = "" }, "service is required"},
		"unknown kind":      {func(in *ReceiveCandidateRequest) { in.Kind = "yaml" }, ""},
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

// The request DTO decodes the same wire field names the application input is
// built from, so the POST /releases body format is pinned here.
func TestReceiveCandidateRequest_DecodesEveryWireField(t *testing.T) {
	raw := `{"service":"core","release_id":"rel-1","image_tag":"img:1","bootstrap":true,` +
		`"repo":"org/r","commit_sha":"sha","kind":"python","shadow":true,` +
		`"source_overlay_uri":"s3://x","verifies_release_id":"rel-0"}`
	var body ReceiveCandidateRequest
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	in := body.toInput()
	assert.Equal(t, "core", in.Service)
	assert.Equal(t, "rel-1", in.ReleaseID)
	assert.Equal(t, "img:1", in.ImageTag)
	assert.True(t, in.Bootstrap)
	assert.Equal(t, "org/r", in.Repo)
	assert.Equal(t, "sha", in.CommitSHA)
	assert.Equal(t, "python", in.Kind)
	assert.True(t, in.Shadow)
	assert.Equal(t, "s3://x", in.SourceOverlayURI)
	assert.Equal(t, "rel-0", in.VerifiesReleaseID)
}

// A release id resubmitted with the same facts is accepted again.
func TestHandleReceiveCandidate_IdenticalResubmitIs202(t *testing.T) {
	deps, _ := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	srv := newTestServer(deps)

	require.Equal(t, http.StatusAccepted, postCandidate(srv, validCandidateInput()).Code)
	require.Equal(t, http.StatusAccepted, postCandidate(srv, validCandidateInput()).Code)
}

// A release id that already names a candidate with different facts is a
// conflict, answered with the reason, and leaves the stored candidate as it was.
func TestHandleReceiveCandidate_ConflictingResubmitIs409(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	srv := newTestServer(deps)
	require.Equal(t, http.StatusAccepted, postCandidate(srv, validCandidateInput()).Code)

	in := validCandidateInput()
	in.ImageTag = "img:2"
	rec := postCandidate(srv, in)

	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, "release id already names a different candidate: \"rel-1\" has a different service, image tag, kind, bootstrap flag or source change\n", rec.Body.String())
	assert.Equal(t, "img:1", releases.releases["rel-1"].ImageTags()["core"])
}

// A candidate that has already left the queue still answers an identical
// resubmit with 202.
func TestHandleReceiveCandidate_IdenticalResubmitOfAnAdvancedCandidateIs202(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	r := pipeline.NewCandidate("rel-1", "core", "img:1", false, "org/r", "sha", release.ManifestKindDbt, now)
	r.SetAssembledImageTags(map[string]string{"core": "img:1", "billing": "img:9"})
	require.NoError(t, r.TransitionToParsing(now))
	releases.releases["rel-1"] = r

	rec := postCandidate(newTestServer(deps), validCandidateInput())

	require.Equal(t, http.StatusAccepted, rec.Code)
}
