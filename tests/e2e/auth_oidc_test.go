package e2e

import (
	"context"
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
