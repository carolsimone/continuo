package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestValidator_Require_present(t *testing.T) {
	t.Setenv("TEST_KEY", "hello")
	v := &Validator{}
	got := v.Require("TEST_KEY")
	if got != "hello" {
		t.Fatalf("want %q, got %q", "hello", got)
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
}

func TestValidator_Require_absent(t *testing.T) {
	os.Unsetenv("ABSENT_KEY")
	v := &Validator{}
	got := v.Require("ABSENT_KEY")
	if got != "" {
		t.Fatalf("want empty string, got %q", got)
	}
	if len(v.Missing()) != 1 || v.Missing()[0] != "ABSENT_KEY" {
		t.Fatalf("want [ABSENT_KEY], got %v", v.Missing())
	}
}

func TestValidator_RequireInt_present(t *testing.T) {
	t.Setenv("TEST_PORT", "8080")
	v := &Validator{}
	got := v.RequireInt("TEST_PORT")
	if got != 8080 {
		t.Fatalf("want 8080, got %d", got)
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
}

func TestValidator_RequireInt_absent(t *testing.T) {
	os.Unsetenv("ABSENT_PORT")
	v := &Validator{}
	got := v.RequireInt("ABSENT_PORT")
	if got != 0 {
		t.Fatalf("want 0, got %d", got)
	}
	if len(v.Missing()) != 1 || v.Missing()[0] != "ABSENT_PORT" {
		t.Fatalf("want [ABSENT_PORT], got %v", v.Missing())
	}
}

func TestValidator_RequireInt_invalid(t *testing.T) {
	t.Setenv("BAD_PORT", "notanumber")
	v := &Validator{}
	got := v.RequireInt("BAD_PORT")
	if got != 0 {
		t.Fatalf("want 0, got %d", got)
	}
	if len(v.Missing()) != 1 || v.Missing()[0] != "BAD_PORT" {
		t.Fatalf("want [BAD_PORT], got %v", v.Missing())
	}
}

func TestValidator_Missing_accumulates_all(t *testing.T) {
	os.Unsetenv("KEY_A")
	os.Unsetenv("KEY_B")
	os.Unsetenv("KEY_C")
	v := &Validator{}
	v.Require("KEY_A")
	v.Require("KEY_B")
	v.Require("KEY_C")
	if got := len(v.Missing()); got != 3 {
		t.Fatalf("want 3 missing, got %d: %v", got, v.Missing())
	}
}

func TestValidator_Add_records_entry(t *testing.T) {
	v := &Validator{}
	v.Add("MY_CUSTOM_VAR (must be foo|bar)")
	missing := v.Missing()
	if len(missing) != 1 || missing[0] != "MY_CUSTOM_VAR (must be foo|bar)" {
		t.Fatalf("want [MY_CUSTOM_VAR (must be foo|bar)], got %v", missing)
	}
}

func TestEnvBoolOrDefault(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		unset    bool
		fallback bool
		want     bool
	}{
		{name: "unset uses fallback true", unset: true, fallback: true, want: true},
		{name: "unset uses fallback false", unset: true, fallback: false, want: false},
		{name: "true", value: "true", want: true},
		{name: "1", value: "1", want: true},
		{name: "True", value: "True", want: true},
		{name: "false", value: "false", want: false},
		{name: "0", value: "0", want: false},
		{name: "unrecognized uses fallback", value: "maybe", fallback: false, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const key = "TEST_BOOL_VAR"
			if tc.unset {
				os.Unsetenv(key)
			} else {
				t.Setenv(key, tc.value)
			}
			got := EnvBoolOrDefault(key, tc.fallback)
			if got != tc.want {
				t.Fatalf("EnvBoolOrDefault(%q, %v) = %v, want %v", tc.value, tc.fallback, got, tc.want)
			}
		})
	}
}

// TestValidatorDurationOrDefault pins the difference between a value an
// operator never set and one they set wrongly.
//
// An unset key is a service running its own default, which is the intended
// shape of every optional duration. A key set to something that is not a Go
// duration is an operator who asked for a specific behaviour and would silently
// get a different one: the install looks configured, and nothing anywhere says
// otherwise. That is a start-up failure naming the key, not a fallback.
func TestValidatorDurationOrDefault(t *testing.T) {
	const key = "TEST_DURATION_VAR"

	t.Run("unset uses the fallback and reports nothing", func(t *testing.T) {
		os.Unsetenv(key)
		v := &Validator{}
		if got := v.DurationOrDefault(key, time.Minute); got != time.Minute {
			t.Errorf("DurationOrDefault = %v, want the fallback %v", got, time.Minute)
		}
		if len(v.Missing()) != 0 {
			t.Errorf("Missing() = %v, want empty: an unset optional key is not a misconfiguration", v.Missing())
		}
	})

	t.Run("a valid duration is used", func(t *testing.T) {
		t.Setenv(key, "90s")
		v := &Validator{}
		if got := v.DurationOrDefault(key, time.Minute); got != 90*time.Second {
			t.Errorf("DurationOrDefault = %v, want 90s", got)
		}
		if len(v.Missing()) != 0 {
			t.Errorf("Missing() = %v, want empty", v.Missing())
		}
	})

	t.Run("an unparseable value fails start-up and names the key", func(t *testing.T) {
		t.Setenv(key, "20 minutes")
		v := &Validator{}
		got := v.DurationOrDefault(key, time.Minute)
		if got != time.Minute {
			t.Errorf("DurationOrDefault = %v, want the fallback so the caller holds a usable value while start-up fails", got)
		}
		missing := v.Missing()
		if len(missing) != 1 {
			t.Fatalf("Missing() = %v, want exactly one recorded failure", missing)
		}
		if !strings.Contains(missing[0], key) {
			t.Errorf("Missing()[0] = %q, want it to name %q so the boot log says which key is wrong", missing[0], key)
		}
	})
}
