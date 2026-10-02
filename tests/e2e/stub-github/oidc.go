package main

// A fake GitHub Actions OIDC issuer for the compose stack and e2e. It publishes
// discovery and a JWKS like token.actions.githubusercontent.com and mints
// RS256 tokens for whatever claims a test posts, so the ui's real bearer
// verification and binding checks run end to end without GitHub.

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"log"
	"math/big"
	"net/http"
	"os"
	"time"
)

const oidcKid = "stub-github-actions"

var oidcKey = mustRSAKey()

func mustRSAKey() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("stub-github: generate OIDC key: %v", err)
	}
	return k
}

func oidcIssuer() string {
	if v := os.Getenv("OIDC_ISSUER"); v != "" {
		return v
	}
	return "http://stub-github:9200"
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func handleOIDCDiscovery(w http.ResponseWriter, _ *http.Request) {
	iss := oidcIssuer()
	writeJSON(w, map[string]any{
		"issuer":                                iss,
		"jwks_uri":                              iss + "/.well-known/jwks",
		"response_types_supported":              []string{"id_token"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
	})
}

func handleOIDCJWKS(w http.ResponseWriter, _ *http.Request) {
	pub := oidcKey.PublicKey
	writeJSON(w, map[string]any{"keys": []map[string]string{{
		"kty": "RSA",
		"alg": "RS256",
		"use": "sig",
		"kid": oidcKid,
		"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}}})
}

// handleMintToken signs a token for the posted audience and claims. The
// response shape matches GitHub's ACTIONS_ID_TOKEN_REQUEST_URL: {"value": jwt}.
func handleMintToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var in struct {
		Audience        string            `json:"audience"`
		Claims          map[string]string `json:"claims"`
		LifetimeSeconds int64             `json:"lifetime_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Audience == "" {
		http.Error(w, "body must be {audience, claims}", http.StatusBadRequest)
		return
	}
	if in.LifetimeSeconds <= 0 {
		in.LifetimeSeconds = 300
	}
	now := time.Now().Unix()
	payload := map[string]any{}
	for k, v := range in.Claims {
		payload[k] = v
	}
	payload["iss"] = oidcIssuer()
	payload["aud"] = in.Audience
	payload["iat"] = now
	payload["nbf"] = now
	payload["exp"] = now + in.LifetimeSeconds
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": oidcKid})
	body, _ := json.Marshal(payload)
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, oidcKey, crypto.SHA256, digest[:])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"value": signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)})
}
