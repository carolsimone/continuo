package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validFixInput() ReceiveFixRequest {
	return ReceiveFixRequest{
		CheckID: "verify-rel-1-core-r1-t1", Project: "core", ImageTag: "img:1", Kind: "dbt",
		VerifiesReleaseID: "rel-1", Round: 1, Try: 1, SourceOverlayURI: "s3://b/core/verify-rel-1-core-r1-t1/source-overlay.tar.gz",
	}
}

func postReleaseCheck(srv *Server, in ReceiveFixRequest) *httptest.ResponseRecorder {
	body, _ := json.Marshal(in)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/release-checks", bytes.NewReader(body))
	srv.Routes().ServeHTTP(rec, req)
	return rec
}

func TestHandleReceiveFix_Accepted(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)

	rec := postReleaseCheck(newTestServer(deps), validFixInput())

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "verify-rel-1-core-r1-t1", body["check_id"])

	stored := releases.releases["verify-rel-1-core-r1-t1"]
	require.NotNil(t, stored)
	assert.Equal(t, releasecheck.KindFix, stored.Kind())
}

func TestHandleReceiveFix_ValidationIs400(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, _ := newRetryRemediationDeps(now)
	for name, mutate := range map[string]func(*ReceiveFixRequest){
		"missing check_id": func(in *ReceiveFixRequest) { in.CheckID = "" },
		"bad kind":         func(in *ReceiveFixRequest) { in.Kind = "yaml" },
		"try below one":    func(in *ReceiveFixRequest) { in.Try = 0 },
		"round below one":  func(in *ReceiveFixRequest) { in.Round = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			in := validFixInput()
			mutate(&in)
			rec := postReleaseCheck(newTestServer(deps), in)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// A fix's release check never has the kind ontology: POST /release-checks
// refuses it as a client error whose body is the message, and stores nothing.
func TestHandleReceiveFix_RefusesOntologyWith400(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	deps.OntologyEnabled = true
	in := validFixInput()
	in.Kind = "ontology"

	rec := postReleaseCheck(newTestServer(deps), in)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, `a fix's release check cannot have kind "ontology"`+"\n", rec.Body.String())
	assert.Empty(t, releases.releases)
}

func TestHandleReceiveFix_CandidateIDConflictIs409(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-9"] = releasecheck.NewCandidate("rel-9", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, now)

	in := validFixInput()
	in.CheckID = "rel-9"
	rec := postReleaseCheck(newTestServer(deps), in)

	require.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandleReceiveCandidate_FixIDConflictIs409(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["check-9"] = releasecheck.NewFix("check-9", "core", "img", "rel-0", 1, 1, "", release.ManifestKindDbt, now)

	body, _ := json.Marshal(ReceiveCandidateRequest{
		Project: "core", ReleaseID: "check-9", ImageTag: "img", Repo: "org/r", CommitSHA: "sha",
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code)
}

// TestHandleReceiveCandidate_RefusesFixFieldsWith400 verifies that
// POST /releases answers 400 for a body carrying any of the three
// fix's release check-only fields, rather than persisting a misrouted check.
func TestHandleReceiveCandidate_RefusesFixFieldsWith400(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	base := ReceiveCandidateRequest{Project: "core", ReleaseID: "rel-1", ImageTag: "img", Repo: "org/r", CommitSHA: "sha"}
	for name, mutate := range map[string]func(*ReceiveCandidateRequest){
		"shadow":              func(in *ReceiveCandidateRequest) { in.Shadow = true },
		"source_overlay_uri":  func(in *ReceiveCandidateRequest) { in.SourceOverlayURI = "s3://x" },
		"verifies_release_id": func(in *ReceiveCandidateRequest) { in.VerifiesReleaseID = "rel-0" },
	} {
		t.Run(name, func(t *testing.T) {
			deps, _ := newRetryRemediationDeps(now)
			in := base
			mutate(&in)
			body, _ := json.Marshal(in)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
			newTestServer(deps).Routes().ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "POST /release-checks")
		})
	}
}

// POST /release-checks reads the wire field names below; a mistyped tag on
// the request DTO would drop a field silently, so the stored check is checked
// against a raw JSON body.
func TestHandleReceiveFix_StoresEveryWireFieldAsSent(t *testing.T) {
	deps, releases := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	raw := `{"check_id":"verify-rel-7-core-r2-t3","project":"core","image_tag":"img:7","kind":"dbt",` +
		`"verifies_release_id":"rel-7","round":2,"try":3,"source_overlay_uri":"s3://b/core/overlay.tar.gz"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/release-checks", strings.NewReader(raw))
	newTestServer(deps).Routes().ServeHTTP(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	stored := releases.releases["verify-rel-7-core-r2-t3"]
	require.NotNil(t, stored)
	assert.Equal(t, releasecheck.KindFix, stored.Kind())
	assert.Equal(t, "rel-7", stored.VerifiesReleaseID())
	assert.Equal(t, 2, stored.Round())
	assert.Equal(t, 3, stored.Try())
	assert.Equal(t, "s3://b/core/overlay.tar.gz", stored.SourceOverlayURI())
	assert.Equal(t, "img:7", stored.ImageTags()["core"])
	assert.Equal(t, release.ManifestKindDbt, stored.ManifestKind())
}
