package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The ui trusts stub-github as its GitHub Actions issuer (tests/e2e/ci-auth.json).
// These repository ids are bound there: dbt fixtures, python fixtures, and one
// service bound without allowBootstrap.
const (
	ciAudience                 = "http://ui:8090"
	e2eDbtRepositoryID         = "100000001"
	e2ePyRepositoryID          = "100000002"
	e2eNoBootstrapRepositoryID = "100000003"
)

// mintCIToken asks stub-github for a GitHub Actions-shaped OIDC token for a
// push to main of the given repository at sha.
func mintCIToken(t *testing.T, repositoryID, repository, sha string) string {
	t.Helper()
	reqBody, err := json.Marshal(map[string]any{
		"audience": ciAudience,
		"claims": map[string]string{
			"sub":           "repo:" + repository + ":ref:refs/heads/main",
			"repository_id": repositoryID,
			"repository":    repository,
			"sha":           sha,
			"ref":           "refs/heads/main",
			"ref_protected": "true",
			"workflow_ref":  repository + "/.github/workflows/release.yml@refs/heads/main",
			"run_id":        "1",
		},
	})
	require.NoError(t, err)
	base := getEnv("STUB_GITHUB_BASE", "http://stub-github:9200")
	resp, err := http.Post(base+"/_test/oidc/token", "application/json", strings.NewReader(string(reqBody)))
	require.NoError(t, err, "mint CI token")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct{ Value string }
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out.Value
}

// submitPublicRelease POSTs to the public release API with a bearer token and
// returns the status and decoded JSON body.
func submitPublicRelease(t *testing.T, clients *testClients, token string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, clients.uiBase+"/api/v1/releases", strings.NewReader(string(raw)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "POST /api/v1/releases")
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(data, &out)
	return resp.StatusCode, out
}
