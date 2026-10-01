package e2e

import (
	"context"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// TestAuthOIDC drives the real OIDC login flow against the auth-e2e compose
// profile (dex + ui-auth). It is skipped unless UI_AUTH_HTTP_BASE is set, so
// the standard e2e run is unaffected.
func TestAuthOIDC(t *testing.T) {
	base := getEnv("UI_AUTH_HTTP_BASE", "")
	if base == "" {
		t.Skip("UI_AUTH_HTTP_BASE not set; start `docker compose --profile auth-e2e up -d dex ui-auth` and set UI_AUTH_HTTP_BASE=http://ui-auth:8090")
	}

	t.Run("healthz is public", func(t *testing.T) {
		if st := statusOf(t, http.DefaultClient, "GET", base+"/healthz"); st != http.StatusOK {
			t.Fatalf("GET /healthz = %d, want 200", st)
		}
	})

	t.Run("unauthenticated API access is rejected", func(t *testing.T) {
		if st := statusOf(t, http.DefaultClient, "GET", base+"/api/schedulers"); st != http.StatusUnauthorized {
			t.Fatalf("GET /api/schedulers without session = %d, want 401", st)
		}
		if st := statusOf(t, http.DefaultClient, "GET", base+"/auth/me"); st != http.StatusUnauthorized {
			t.Fatalf("GET /auth/me without session = %d, want 401", st)
		}
	})

	t.Run("viewer can read but not mutate", func(t *testing.T) {
		client, _ := loginThroughDex(t, base, "viewer@example.com", "password")
		if st := statusOf(t, client, "GET", base+"/api/schedulers"); st != http.StatusOK {
			t.Fatalf("viewer GET /api/schedulers = %d, want 200", st)
		}
		if st := statusOf(t, client, "POST", base+"/api/schedules/any/trigger"); st != http.StatusForbidden {
			t.Fatalf("viewer POST trigger = %d, want 403", st)
		}
	})

	t.Run("operator clears the auth gates on mutations", func(t *testing.T) {
		client, _ := loginThroughDex(t, base, "operator@example.com", "password")
		st := statusOf(t, client, "POST", base+"/api/schedules/any/trigger")
		// The schedule does not exist, so the domain layer may answer 4xx/5xx;
		// the assertion is that BOTH auth gates passed.
		if st == http.StatusUnauthorized || st == http.StatusForbidden {
			t.Fatalf("operator POST trigger = %d, auth gate should have passed", st)
		}
	})

	t.Run("user with no mapped role is denied at callback", func(t *testing.T) {
		client, finalURL := loginThroughDex(t, base, "norole@example.com", "password")
		if !strings.Contains(finalURL.String(), "auth_error=no_role") {
			t.Fatalf("norole login landed on %s, want auth_error=no_role", finalURL)
		}
		if st := statusOf(t, client, "GET", base+"/auth/me"); st != http.StatusUnauthorized {
			t.Fatalf("norole /auth/me = %d, want 401", st)
		}
	})

	dexToken := func(t *testing.T, email string) string {
		t.Helper()
		form := url.Values{"grant_type": {"password"}, "scope": {"openid email profile"}, "username": {email}, "password": {"password"}}
		req, _ := http.NewRequest(http.MethodPost, getEnv("DEX_BASE", "http://dex:5556/dex")+"/token", strings.NewReader(form.Encode()))
		req.SetBasicAuth("continuo-ui", "e2e-secret")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("dex password grant: %v", err)
		}
		defer resp.Body.Close()
		var out struct {
			IDToken string `json:"id_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.IDToken == "" {
			t.Fatalf("dex password grant: status %d, no id_token", resp.StatusCode)
		}
		return out.IDToken
	}
	bearer := func(t *testing.T, method, path, token, body string) int {
		t.Helper()
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	t.Run("operator Dex bearer clears the submit gates", func(t *testing.T) {
		tok := dexToken(t, "operator@example.com")
		if st := bearer(t, "GET", "/api/v1/current-prod", tok, ""); st != http.StatusOK {
			t.Fatalf("operator GET current-prod = %d, want 200", st)
		}
		// image_tag is empty: the auth gates pass and the body validation
		// refuses it, so no release is created.
		if st := bearer(t, "POST", "/api/v1/releases", tok, `{"release_id":"auth-e2e","service":"service-1","image_tag":""}`); st != http.StatusBadRequest {
			t.Fatalf("operator POST = %d, want 400 from body validation", st)
		}
	})

	t.Run("viewer Dex bearer cannot submit; no-role bearer is refused", func(t *testing.T) {
		if st := bearer(t, "POST", "/api/v1/releases", dexToken(t, "viewer@example.com"), `{"release_id":"r","service":"service-1","image_tag":"t"}`); st != http.StatusForbidden {
			t.Fatalf("viewer POST = %d, want 403", st)
		}
		if st := bearer(t, "GET", "/api/v1/current-prod", dexToken(t, "norole@example.com"), ""); st != http.StatusForbidden {
			t.Fatalf("norole GET = %d, want 403", st)
		}
	})

	t.Run("deleting the redis session revokes access instantly", func(t *testing.T) {
		client, _ := loginThroughDex(t, base, "operator@example.com", "password")
		if st := statusOf(t, client, "GET", base+"/auth/me"); st != http.StatusOK {
			t.Fatalf("operator /auth/me before revocation = %d, want 200", st)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rdb := goredis.NewClient(&goredis.Options{
			Addr:     getEnv("REDIS_HOST", "redis") + ":6379",
			Password: getEnv("REDIS_PASSWORD", "continuo"),
		})
		defer rdb.Close()
		iter := rdb.Scan(ctx, 0, "uisession:*", 100).Iterator()
		deleted := 0
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
			deleted++
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("redis scan: %v", err)
		}
		if deleted == 0 {
			t.Fatal("expected at least one uisession:* key to delete")
		}

		if st := statusOf(t, client, "GET", base+"/auth/me"); st != http.StatusUnauthorized {
			t.Fatalf("operator /auth/me after revocation = %d, want 401", st)
		}
		if st := statusOf(t, client, "GET", base+"/api/schedulers"); st != http.StatusUnauthorized {
			t.Fatalf("operator API read after revocation = %d, want 401", st)
		}
	})
}

func statusOf(t *testing.T, client *http.Client, method, target string) int {
	t.Helper()
	req, err := http.NewRequest(method, target, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build %s %s: %v", method, target, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

var formActionRe = regexp.MustCompile(`action="([^"]+)"`)

// loginThroughDex performs the full browser flow: ui /auth/login redirect →
// dex login form → credential POST → dex redirects → ui /auth/callback →
// final redirect into the SPA. Returns the cookie-jar client and the URL the
// flow finally landed on.
func loginThroughDex(t *testing.T, base, email, password string) (*http.Client, *url.URL) {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	client := &http.Client{Jar: jar, Timeout: 30 * time.Second}

	resp, err := client.Get(base + "/auth/login?returnTo=/")
	if err != nil {
		t.Fatalf("GET /auth/login: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected dex login form, got %d: %s", resp.StatusCode, body)
	}
	m := formActionRe.FindSubmatch(body)
	if m == nil {
		t.Fatalf("no form action in dex login page:\n%s", body)
	}
	actionURL, err := resp.Request.URL.Parse(html.UnescapeString(string(m[1])))
	if err != nil {
		t.Fatalf("resolve form action: %v", err)
	}

	resp2, err := client.PostForm(actionURL.String(), url.Values{"login": {email}, "password": {password}})
	if err != nil {
		t.Fatalf("POST dex credentials: %v", err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if !strings.HasPrefix(resp2.Request.URL.String(), base) {
		t.Fatalf("login flow ended at %s, expected to land back on %s", resp2.Request.URL, base)
	}
	return client, resp2.Request.URL
}
