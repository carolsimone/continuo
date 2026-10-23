package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadTaskTimeout(t *testing.T) {
	cases := []struct {
		raw   string
		want  time.Duration
		valid bool
	}{
		{raw: "", want: 24 * time.Hour, valid: true},
		{raw: "3600", want: time.Hour, valid: true},
		{raw: "86400", want: 24 * time.Hour, valid: true},
		{raw: "604800", want: 168 * time.Hour, valid: true},
		{raw: "3599"},
		{raw: "604801"},
		{raw: "24h"},
		// 18446747674 s is 2^64 ns plus about an hour: refused, though its
		// nanoseconds would wrap into the range.
		{raw: "18446747674"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv(TaskTimeoutEnvKey, tc.raw)
			v := &Validator{}
			got := LoadTaskTimeout(v)
			invalid := strings.Contains(strings.Join(v.Missing(), " "), TaskTimeoutEnvKey)
			if tc.valid {
				if invalid || got != tc.want {
					t.Fatalf("LoadTaskTimeout(%q) = %v, invalid %v; want %v", tc.raw, got, invalid, tc.want)
				}
				return
			}
			if !invalid {
				t.Fatalf("LoadTaskTimeout(%q) accepted the value", tc.raw)
			}
		})
	}
}
