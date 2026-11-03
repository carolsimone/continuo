package config

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func writeSecretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func containsEntry(missing []string, want string) bool {
	for _, m := range missing {
		if m == want {
			return true
		}
	}
	return false
}

func TestRequireSecretFromEnv(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "pw")
	t.Setenv("POSTGRES_PASSWORD_FILE", "")
	v := &Validator{}
	s := v.RequireSecret("POSTGRES_PASSWORD")
	got, err := s.Value()
	if err != nil || got != "pw" {
		t.Fatalf("Value() = %q, %v; want pw, nil", got, err)
	}
	if !s.IsSet() || s.Name() != "POSTGRES_PASSWORD" {
		t.Fatalf("IsSet=%v Name=%q", s.IsSet(), s.Name())
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("missing = %v", v.Missing())
	}
}

func TestRequireSecretFromFile(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", writeSecretFile(t, "pw\n"))
	v := &Validator{}
	s := v.RequireSecret("POSTGRES_PASSWORD")
	got, err := s.Value()
	if err != nil || got != "pw" {
		t.Fatalf("Value() = %q, %v; want pw, nil", got, err)
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("missing = %v", v.Missing())
	}
}

func TestRequireSecretBothSetIsReported(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "pw")
	t.Setenv("POSTGRES_PASSWORD_FILE", writeSecretFile(t, "pw"))
	v := &Validator{}
	v.RequireSecret("POSTGRES_PASSWORD")
	want := "POSTGRES_PASSWORD: set POSTGRES_PASSWORD or POSTGRES_PASSWORD_FILE, not both"
	if !containsEntry(v.Missing(), want) {
		t.Fatalf("missing = %v; want entry %q", v.Missing(), want)
	}
	if containsEntry(v.Missing(), "POSTGRES_PASSWORD") {
		t.Fatalf("missing = %v; the bare name must not be recorded beside the both-forms problem", v.Missing())
	}
}

func TestRequireSecretMissing(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", "")
	v := &Validator{}
	s := v.RequireSecret("POSTGRES_PASSWORD")
	if m := v.Missing(); len(m) != 1 || m[0] != "POSTGRES_PASSWORD" {
		t.Fatalf("missing = %v; want exactly [POSTGRES_PASSWORD]", m)
	}
	if s.IsSet() {
		t.Fatal("IsSet() = true for a missing secret")
	}
}

func TestOptionalSecretUnset(t *testing.T) {
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_API_KEY_FILE", "")
	v := &Validator{}
	s := v.OptionalSecret("LLM_API_KEY")
	if s.IsSet() {
		t.Fatal("IsSet() = true")
	}
	got, err := s.Value()
	if got != "" || err != nil {
		t.Fatalf("Value() = %q, %v; want empty, nil", got, err)
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("missing = %v", v.Missing())
	}
}

func TestOptionalSecretBothSetIsReported(t *testing.T) {
	t.Setenv("LLM_API_KEY", "k")
	t.Setenv("LLM_API_KEY_FILE", writeSecretFile(t, "k"))
	v := &Validator{}
	v.OptionalSecret("LLM_API_KEY")
	want := "LLM_API_KEY: set LLM_API_KEY or LLM_API_KEY_FILE, not both"
	if !containsEntry(v.Missing(), want) {
		t.Fatalf("missing = %v; want entry %q", v.Missing(), want)
	}
}

func TestSecretFileTrimsOneTrailingNewline(t *testing.T) {
	cases := []struct{ name, content, want string }{
		{"lf", "pw\n", "pw"},
		{"crlf", "pw\r\n", "pw"},
		{"two lf", "pw\n\n", "pw\n"},
		{"no newline", "pw", "pw"},
		{"special characters", "p w'@:\\", "p w'@:\\"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_PASSWORD_FILE", writeSecretFile(t, tc.content))
			v := &Validator{}
			got, err := v.RequireSecret("POSTGRES_PASSWORD").Value()
			if err != nil || got != tc.want {
				t.Fatalf("Value() = %q, %v; want %q, nil", got, err, tc.want)
			}
		})
	}
}

func TestSecretFileIsReadAgainOnEveryValue(t *testing.T) {
	p := writeSecretFile(t, "a")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", p)
	v := &Validator{}
	s := v.RequireSecret("POSTGRES_PASSWORD")
	if got, _ := s.Value(); got != "a" {
		t.Fatalf("first Value() = %q; want a", got)
	}
	if err := os.WriteFile(p, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Value(); got != "b" {
		t.Fatalf("second Value() = %q; want b", got)
	}
}

func TestSecretValueFailsWhenTheFileGoesAway(t *testing.T) {
	p := writeSecretFile(t, "a")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", p)
	s := (&Validator{}).RequireSecret("POSTGRES_PASSWORD")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Value(); err == nil || got != "" {
		t.Fatalf("Value() = %q, %v; want an error", got, err)
	}
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Value(); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("Value() err = %v; want an is-empty error", err)
	}
}

func TestSecretEmptyFileIsReported(t *testing.T) {
	for _, content := range []string{"", "\n", "\r\n"} {
		t.Run(fmt.Sprintf("%q", content), func(t *testing.T) {
			p := writeSecretFile(t, content)
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_PASSWORD_FILE", p)
			v := &Validator{}
			v.RequireSecret("POSTGRES_PASSWORD")
			want := "POSTGRES_PASSWORD_FILE: " + p + " is empty"
			if !containsEntry(v.Missing(), want) {
				t.Fatalf("missing = %v; want entry %q", v.Missing(), want)
			}
		})
	}
}

func TestSecretMissingFileIsReported(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", filepath.Join(t.TempDir(), "absent"))
	v := &Validator{}
	v.RequireSecret("POSTGRES_PASSWORD")
	m := v.Missing()
	if len(m) != 1 || !strings.HasPrefix(m[0], "POSTGRES_PASSWORD_FILE: open ") {
		t.Fatalf("missing = %v; want one entry starting POSTGRES_PASSWORD_FILE: open ", m)
	}
}

func TestSecretValueHelpers(t *testing.T) {
	t.Setenv("NEO4J_PASSWORD", "")
	t.Setenv("NEO4J_PASSWORD_FILE", writeSecretFile(t, "n4j\n"))
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_API_KEY_FILE", "")
	v := &Validator{}
	if got := v.RequireSecretValue("NEO4J_PASSWORD"); got != "n4j" {
		t.Fatalf("RequireSecretValue = %q; want n4j", got)
	}
	if got := v.OptionalSecretValue("LLM_API_KEY"); got != "" {
		t.Fatalf("OptionalSecretValue = %q; want empty", got)
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("missing = %v", v.Missing())
	}
}

func TestSecretValueHelpersReturnEmptyWhenReported(t *testing.T) {
	t.Setenv("NEO4J_PASSWORD", "x")
	t.Setenv("NEO4J_PASSWORD_FILE", writeSecretFile(t, "y"))
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_API_KEY_FILE", writeSecretFile(t, ""))
	v := &Validator{}
	if got := v.RequireSecretValue("NEO4J_PASSWORD"); got != "" {
		t.Fatalf("RequireSecretValue = %q; want empty", got)
	}
	if got := v.OptionalSecretValue("LLM_API_KEY"); got != "" {
		t.Fatalf("OptionalSecretValue = %q; want empty", got)
	}
	if len(v.Missing()) != 2 {
		t.Fatalf("missing = %v; want 2 entries", v.Missing())
	}
}

func TestSecretValueIsSafeForConcurrentUse(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_PASSWORD_FILE", writeSecretFile(t, "pw\n"))
	s := (&Validator{}).RequireSecret("POSTGRES_PASSWORD")
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if got, err := s.Value(); err != nil || got != "pw" {
					t.Errorf("Value() = %q, %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestCredentialVarsAreTheClosedList(t *testing.T) {
	want := []string{
		"POSTGRES_PASSWORD", "REDIS_PASSWORD", "NEO4J_PASSWORD",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "LLM_API_KEY",
		"GITHUB_TOKEN", "GITHUB_APP_PRIVATE_KEY", "AUTH_OIDC_CLIENT_SECRET",
	}
	if strings.Join(CredentialVars, ",") != strings.Join(want, ",") {
		t.Fatalf("CredentialVars = %v", CredentialVars)
	}
}

func TestSecretNeverFormatsItsValue(t *testing.T) {
	const credential = "hunter2-credential"
	t.Setenv("POSTGRES_PASSWORD", credential)
	t.Setenv("POSTGRES_PASSWORD_FILE", "")
	s := (&Validator{}).RequireSecret("POSTGRES_PASSWORD")
	holder := struct{ Password Secret }{Password: s}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		for _, operand := range []any{s, &s, holder} {
			got := fmt.Sprintf(verb, operand)
			if strings.Contains(got, credential) {
				t.Errorf("Sprintf(%q, %T) = %q; it carries the credential", verb, operand, got)
			}
			if !strings.Contains(got, "[redacted]") {
				t.Errorf("Sprintf(%q, %T) = %q; want [redacted]", verb, operand, got)
			}
		}
	}

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", "password", s, "holder", holder)
	if strings.Contains(buf.String(), credential) {
		t.Errorf("slog output %q carries the credential", buf.String())
	}
	if !strings.Contains(buf.String(), `"password":"[redacted]"`) {
		t.Errorf("slog output %q; want \"password\":\"[redacted]\"", buf.String())
	}
}

func TestUnsetSecretFormatsEmpty(t *testing.T) {
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_API_KEY_FILE", "")
	s := (&Validator{}).OptionalSecret("LLM_API_KEY")
	if got := fmt.Sprintf("%v|%#v", s, s); got != "|" {
		t.Errorf("unset Secret formats as %q; want empty", got)
	}
	if got := s.LogValue().String(); got != "" {
		t.Errorf("unset Secret LogValue = %q; want empty", got)
	}
}
