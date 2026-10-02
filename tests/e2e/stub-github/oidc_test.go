package main

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOIDC_MintedTokenVerifiesAgainstJWKS(t *testing.T) {
	disc := httptest.NewRecorder()
	handleOIDCDiscovery(disc, httptest.NewRequest(http.MethodGet, "/.well-known/openid-configuration", nil))
	var meta struct {
		Issuer  string `json:"issuer"`
		JwksURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(disc.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Issuer != oidcIssuer() || meta.JwksURI != oidcIssuer()+"/.well-known/jwks" {
		t.Fatalf("discovery = %+v", meta)
	}

	mint := httptest.NewRecorder()
	body := `{"audience":"http://ui:8090","claims":{"repository_id":"100000001","repository":"carolsimone/continuo-demo","sha":"abc"}}`
	handleMintToken(mint, httptest.NewRequest(http.MethodPost, "/_test/oidc/token", strings.NewReader(body)))
	if mint.Code != http.StatusOK {
		t.Fatalf("mint status %d: %s", mint.Code, mint.Body.String())
	}
	var tok struct{ Value string }
	_ = json.Unmarshal(mint.Body.Bytes(), &tok)
	parts := strings.Split(tok.Value, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWT: %q", tok.Value)
	}

	jw := httptest.NewRecorder()
	handleOIDCJWKS(jw, httptest.NewRequest(http.MethodGet, "/.well-known/jwks", nil))
	var set struct {
		Keys []struct{ Kid, N, E string } `json:"keys"`
	}
	_ = json.Unmarshal(jw.Body.Bytes(), &set)
	n, _ := base64.RawURLEncoding.DecodeString(set.Keys[0].N)
	e, _ := base64.RawURLEncoding.DecodeString(set.Keys[0].E)
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}

	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	_ = json.Unmarshal(payload, &claims)
	if claims["iss"] != oidcIssuer() || claims["aud"] != "http://ui:8090" || claims["repository_id"] != "100000001" {
		t.Fatalf("claims = %v", claims)
	}
	if claims["exp"].(float64)-claims["iat"].(float64) != 300 {
		t.Fatalf("default lifetime should be 300s, claims = %v", claims)
	}
}

func TestOIDC_MintRejectsGET(t *testing.T) {
	rec := httptest.NewRecorder()
	handleMintToken(rec, httptest.NewRequest(http.MethodGet, "/_test/oidc/token", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", rec.Code)
	}
}
