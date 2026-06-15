package config

import (
	"os"
	"testing"
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
