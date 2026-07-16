package http_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorBody is the stable error envelope every rejection carries.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func decodeError(t *testing.T, resp response) errorBody {
	t.Helper()
	var body errorBody
	require.NoError(t, json.Unmarshal(resp.Body, &body))
	return body
}

// authenticatedPaths are every internal endpoint. None of them may serve an
// unauthenticated caller.
var authenticatedPaths = []struct {
	method, path, body string
}{
	{http.MethodGet, "/internal/v1/worker/runtime", ""},
	{http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":0}`},
	{http.MethodPost, "/internal/v1/workers/initialization", `{"ok":true}`},
	{http.MethodPost, "/internal/v1/leases/44444444-4444-4444-4444-444444444444/start",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333"}`},
	{http.MethodPost, "/internal/v1/leases/44444444-4444-4444-4444-444444444444/heartbeat",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333"}`},
	{http.MethodPost, "/internal/v1/leases/44444444-4444-4444-4444-444444444444/result-urls",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333"}`},
	{http.MethodPost, "/internal/v1/leases/44444444-4444-4444-4444-444444444444/complete",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333","result":{"succeeded":true}}`},
}

// TestAuth_MissingCredentialIsRejectedOnEveryEndpoint is the test that must
// fail if the credential check is removed from any handler.
func TestAuth_MissingCredentialIsRejectedOnEveryEndpoint(t *testing.T) {
	for _, ep := range authenticatedPaths {
		t.Run(ep.path, func(t *testing.T) {
			r := newRig(t)
			r.grant()

			resp := r.do(ep.method, ep.path, ep.body, noCredential)

			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			assert.Equal(t, "unauthenticated", decodeError(t, resp).Error.Code)
			// The rejected call never reached the lease service.
			assert.Zero(t, r.leases.claims+r.leases.starts+r.leases.beats+r.leases.completes+r.leases.tasks)
			assert.Empty(t, r.pools.reports)
		})
	}
}

func TestAuth_WrongCredentialIsRejectedOnEveryEndpoint(t *testing.T) {
	for _, ep := range authenticatedPaths {
		t.Run(ep.path, func(t *testing.T) {
			r := newRig(t)
			r.grant()

			resp := r.do(ep.method, ep.path, ep.body, wrongCredential)

			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			assert.Equal(t, "unauthenticated", decodeError(t, resp).Error.Code)
			assert.Zero(t, r.leases.claims+r.leases.starts+r.leases.beats+r.leases.completes+r.leases.tasks)
		})
	}
}

// TestAuth_UnknownPoolAnswersLikeAWrongCredential keeps a caller from learning
// which pools exist by trying keys.
func TestAuth_UnknownPoolAnswersLikeAWrongCredential(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "", unknownPool)

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "unauthenticated", decodeError(t, resp).Error.Code)
}

// TestAuth_MissingPoolKeyHeaderIsRejected. The credential alone names no pool,
// so a request without the header cannot be authenticated at all.
func TestAuth_MissingPoolKeyHeaderIsRejected(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "", func(req *http.Request) {
		req.Header.Del("X-Continuo-Pool-Key")
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestAuth_NonBearerAuthorizationIsRejected pins the one scheme the API takes.
func TestAuth_NonBearerAuthorizationIsRejected(t *testing.T) {
	cases := map[string]string{
		"basic":         "Basic " + testCredential,
		"bare token":    testCredential,
		"empty bearer":  "Bearer ",
		"bearer spaces": "Bearer    ",
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)

			resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "", func(req *http.Request) {
				req.Header.Set("Authorization", header)
			})

			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		})
	}
}

// TestAuth_BearerSchemeIsCaseInsensitive follows RFC 7235: the scheme token is
// compared case-insensitively, so a conforming client is not rejected.
func TestAuth_BearerSchemeIsCaseInsensitive(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "", func(req *http.Request) {
		req.Header.Set("Authorization", "bearer "+testCredential)
	})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestAuth_LookupFailureIsNotARejection keeps a database outage from telling a
// worker its credential is wrong: one is retriable, the other never is.
func TestAuth_LookupFailureIsNotARejection(t *testing.T) {
	r := newRig(t)
	r.auth.err = errBoom

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "")

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "internal", decodeError(t, resp).Error.Code)
}

// TestAuth_CredentialNeverReachesALogLine. The raw credential is a secret the
// executor only ever compares against a digest; it must not survive in a log.
func TestAuth_CredentialNeverReachesALogLine(t *testing.T) {
	r := newRig(t)

	r.do(http.MethodGet, "/internal/v1/worker/runtime", "")
	r.do(http.MethodGet, "/internal/v1/worker/runtime", "", wrongCredential)
	r.do(http.MethodGet, "/internal/v1/worker/runtime", "", noCredential)

	assert.NotContains(t, r.logs.String(), testCredential)
	assert.NotContains(t, r.logs.String(), "not-the-credential")
	assert.NotContains(t, r.logs.String(), "Bearer")
}

// TestAuth_ErrorBodyNeverEchoesTheCredential keeps the secret out of a response
// a caller (or an intermediary logging bodies) can read back.
func TestAuth_ErrorBodyNeverEchoesTheCredential(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "", wrongCredential)

	body := decodeError(t, resp)
	assert.NotContains(t, body.Error.Message, "not-the-credential")
	assert.NotContains(t, body.Error.Message, testCredential)
}
