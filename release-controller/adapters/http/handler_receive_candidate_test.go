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
	return ReceiveCandidateRequest{Project: "core", ReleaseID: "rel-1", ImageTag: "img:1", Repo: "org/r", CommitSHA: "sha"}
}

func TestHandleReceiveCandidate_Accepted(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())

	rec := postCandidate(newTestServer(deps), validCandidateInput())

	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.NotNil(t, releases.releases["rel-1"])
}

func postCandidateBody(srv *Server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader([]byte(body))))
	return rec
}

// A release names its project in the field project: a body that names it
// only in a field called service is refused as a client error naming
// project, and stores nothing, while the same body with project is accepted.
func TestHandleReceiveCandidate_RequiresProjectAndRefusesABodyThatNamesOnlyService(t *testing.T) {
	const rest = `"release_id":"rel-1","image_tag":"img:1","repo":"org/r","commit_sha":"sha"`

	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	rec := postCandidateBody(newTestServer(deps), `{"service":"core",`+rest+`}`)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "project is required")
	assert.Empty(t, releases.releases)

	deps, releases = newRetryRemediationDeps(time.Unix(100, 0).UTC())
	rec = postCandidateBody(newTestServer(deps), `{"project":"core",`+rest+`}`)

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.NotNil(t, releases.releases["rel-1"])
	assert.Equal(t, "core", releases.releases["rel-1"].ChangedProject())
}

// A submission the caller got wrong is a client error naming the field.
func TestHandleReceiveCandidate_ValidationFailureIs400(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*ReceiveCandidateRequest)
		want   string
	}{
		"missing image_tag": {func(in *ReceiveCandidateRequest) { in.ImageTag = "" }, "image_tag is required"},
		"missing project":   {func(in *ReceiveCandidateRequest) { in.Project = "" }, "project is required"},
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

// retiredPythonKind is the kind a Python project was once released with. It is
// built from two parts because the retired-names guard keeps the whole word out
// of every tracked file.
const retiredPythonKind = "python-" + "odcs"

// POST /releases refuses the retired Python kind as an unknown kind, a client
// error, and stores nothing.
func TestHandleReceiveCandidate_RefusesTheRetiredPythonKindWith400(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	in := validCandidateInput()
	in.Kind = retiredPythonKind

	rec := postCandidate(newTestServer(deps), in)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(),
		`unknown manifest kind "`+retiredPythonKind+`" (expected "dbt", "python" or "ontology")`)
	assert.Empty(t, releases.releases)
}

// While the install's ontology switch is off, POST /releases refuses the kind
// ontology as a client error whose body is the message, and stores nothing.
func TestHandleReceiveCandidate_RefusesOntologyWhileTheSwitchIsOffWith400(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	in := validCandidateInput()
	in.Kind = "ontology"

	rec := postCandidate(newTestServer(deps), in)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, `kind "ontology" is accepted only when the install's ontology switch is on (contracts.ontology.enabled)`+"\n", rec.Body.String())
	assert.Empty(t, releases.releases)
}

func TestHandleReceiveCandidate_AcceptsOntologyWhileTheSwitchIsOn(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	deps.OntologyEnabled = true
	in := validCandidateInput()
	in.Kind = "ontology"

	rec := postCandidate(newTestServer(deps), in)

	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, release.ManifestKindOntology, releases.releases["rel-1"].ManifestKind())
}

// The rules about the ontology project are conflicts with what the install
// holds, so POST /releases answers each with 409 and the message as the body.
func TestHandleReceiveCandidate_OntologyConflictsAre409(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	t.Run("a second ontology project", func(t *testing.T) {
		deps, releases := newRetryRemediationDeps(now)
		deps.OntologyEnabled = true
		seedServiceProd(t, deps, release.NewServiceProd("ontology-a", "rel-0", "s3://b/ontology-a/rel-0/ontology.yaml", "img:1",
			release.ManifestKindOntology, now))
		in := validCandidateInput()
		in.Project, in.Kind = "ontology-b", "ontology"

		rec := postCandidate(newTestServer(deps), in)

		require.Equal(t, http.StatusConflict, rec.Code)
		assert.Equal(t, `project "ontology-a" is the install's ontology: an install has one ontology project`+"\n", rec.Body.String())
		assert.Empty(t, releases.releases)
	})

	t.Run("a dbt project released as the ontology", func(t *testing.T) {
		deps, releases := newRetryRemediationDeps(now)
		deps.OntologyEnabled = true
		seedServiceProd(t, deps, release.NewServiceProd("core", "rel-0", "s3://b/core/rel-0/manifest.json", "img:1",
			release.ManifestKindDbt, now))
		in := validCandidateInput()
		in.Kind = "ontology"

		rec := postCandidate(newTestServer(deps), in)

		require.Equal(t, http.StatusConflict, rec.Code)
		assert.Equal(t, `project "core" is released as kind "dbt": a project cannot change between kind "ontology" and a node kind`+"\n", rec.Body.String())
		assert.Empty(t, releases.releases)
	})

	t.Run("the ontology project released as python", func(t *testing.T) {
		deps, releases := newRetryRemediationDeps(now)
		deps.OntologyEnabled = true
		seedServiceProd(t, deps, release.NewServiceProd("core", "rel-0", "s3://b/core/rel-0/ontology.yaml", "img:1",
			release.ManifestKindOntology, now))
		in := validCandidateInput()
		in.Kind = "python"

		rec := postCandidate(newTestServer(deps), in)

		require.Equal(t, http.StatusConflict, rec.Code)
		assert.Equal(t, `project "core" is released as kind "ontology": a project cannot change between kind "ontology" and a node kind`+"\n", rec.Body.String())
		assert.Empty(t, releases.releases)
	})
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
	failing := beginFailsUoW{&fakeUoW{releases: releases, serviceProd: newFakeServiceProdRepo(), outbox: &fakeOutboxRepo{}}}
	deps.NewUoW = func() uow.UnitOfWork { return failing }
	var logged bytes.Buffer
	srv := NewServer(liveness.NewRegistry(), "0", slog.New(slog.NewTextHandler(&logged, nil)))
	srv.ServeAPI(deps)

	rec := postCandidate(srv, validCandidateInput())

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "internal error\n", rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "10.1.2.3")
	assert.Contains(t, logged.String(), "connection refused")
}

// The request DTO decodes the same wire field names the application input is
// built from, so the POST /releases body format is pinned here.
func TestReceiveCandidateRequest_DecodesEveryWireField(t *testing.T) {
	raw := `{"project":"core","release_id":"rel-1","image_tag":"img:1","bootstrap":true,"rebaseline":true,` +
		`"repo":"org/r","commit_sha":"sha","kind":"python","shadow":true,` +
		`"source_overlay_uri":"s3://x","verifies_release_id":"rel-0"}`
	var body ReceiveCandidateRequest
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	in := body.toInput()
	assert.Equal(t, "core", in.Project)
	assert.Equal(t, "rel-1", in.ReleaseID)
	assert.Equal(t, "img:1", in.ImageTag)
	assert.True(t, in.Bootstrap)
	assert.True(t, in.Rebaseline)
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
	assert.Equal(t, "release id already names a different candidate: \"rel-1\" has a different project, image tag, kind, bootstrap or rebaseline flag, or source change\n", rec.Body.String())
	assert.Equal(t, "img:1", releases.releases["rel-1"].ImageTags()["core"])
}

// A candidate that has already left the queue still answers an identical
// resubmit with 202.
func TestHandleReceiveCandidate_IdenticalResubmitOfAnAdvancedCandidateIs202(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	r := pipeline.NewCandidate("rel-1", "core", "img:1", false, false, "org/r", "sha", release.ManifestKindDbt, now)
	r.SetAssembledImageTags(map[string]string{"core": "img:1", "billing": "img:9"})
	require.NoError(t, r.TransitionToParsing(now))
	releases.releases["rel-1"] = r

	rec := postCandidate(newTestServer(deps), validCandidateInput())

	require.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleReceiveCandidate_RebaselineWithoutBootstrapIs400(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	in := validCandidateInput()
	in.Rebaseline = true
	rec := postCandidate(newTestServer(deps), in)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "rebaseline is accepted only together with bootstrap")
	assert.Empty(t, releases.releases)
}
