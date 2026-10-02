package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPublicAPI_Authorization exercises the public release API's refusals
// through the real verifier and bindings (tests/e2e/ci-auth.json).
func TestPublicAPI_Authorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	body := map[string]any{"service": "service-1", "release_id": "e2e-public-api-refused", "image_tag": "unused"}

	t.Run("an unbound repository is forbidden", func(t *testing.T) {
		token := mintCIToken(t, "999999999", "someone/unbound", "abc")
		status, out := submitPublicRelease(t, clients, token, body)
		require.Equal(t, http.StatusForbidden, status)
		require.Equal(t, "forbidden", out["code"])
	})

	t.Run("bootstrap needs allowBootstrap on the binding", func(t *testing.T) {
		token := mintCIToken(t, e2eNoBootstrapRepositoryID, "carolsimone/no-bootstrap", "abc")
		status, out := submitPublicRelease(t, clients, token, map[string]any{
			"service": "e2e-ci-no-bootstrap", "release_id": "e2e-public-api-boot", "image_tag": "x", "bootstrap": true,
		})
		require.Equal(t, http.StatusForbidden, status)
		require.Equal(t, "bootstrap_not_allowed", out["code"])
	})

	t.Run("a body contradicting the token's repository is claim_mismatch", func(t *testing.T) {
		token := mintCIToken(t, e2eDbtRepositoryID, "carolsimone/continuo-demo", "abc")
		b := map[string]any{"repo": "someone/else"}
		for k, v := range body {
			b[k] = v
		}
		status, out := submitPublicRelease(t, clients, token, b)
		require.Equal(t, http.StatusForbidden, status)
		require.Equal(t, "claim_mismatch", out["code"])
	})

	t.Run("a garbage token is 401", func(t *testing.T) {
		status, out := submitPublicRelease(t, clients, "garbage", body)
		require.Equal(t, http.StatusUnauthorized, status)
		require.Equal(t, "invalid_token", out["code"])
	})

	t.Run("a CI token reads current-prod", func(t *testing.T) {
		token := mintCIToken(t, e2eDbtRepositoryID, "carolsimone/continuo-demo", "abc")
		req, err := http.NewRequest(http.MethodGet, clients.uiBase+"/api/v1/current-prod", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var cp map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&cp))
		require.Contains(t, cp, "current_prod_release_id")
	})
}
