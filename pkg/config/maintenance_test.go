package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadMaintenance_AcceptsExactlyTrueOrFalse(t *testing.T) {
	for raw, want := range map[string]bool{"true": true, "false": false} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(MaintenanceEnvKey, raw)
			v := &Validator{}
			if got := LoadMaintenance(v); got != want {
				t.Fatalf("LoadMaintenance(%q) = %v, want %v", raw, got, want)
			}
			if len(v.Missing()) != 0 {
				t.Fatalf("want no problems, got %v", v.Missing())
			}
		})
	}
}

// A value that is almost a boolean must stop the service rather than be read
// as on or off: the operator meant something, and guessing which risks either
// accepting work during an upgrade or refusing it in production.
func TestLoadMaintenance_RefusesEverythingElse(t *testing.T) {
	for _, raw := range []string{"", "True", "TRUE", "FALSE", "1", "0", "yes", "no", "on", "off", " true", "true ", "enabled"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(MaintenanceEnvKey, raw)
			v := &Validator{}
			LoadMaintenance(v)
			if len(v.Missing()) != 1 || !strings.HasPrefix(v.Missing()[0], MaintenanceEnvKey) {
				t.Fatalf("LoadMaintenance(%q): want one problem naming %s, got %v", raw, MaintenanceEnvKey, v.Missing())
			}
		})
	}
}

func TestLoadMaintenance_RefusesUnset(t *testing.T) {
	t.Setenv(MaintenanceEnvKey, "x") // registers restoration of the original value
	os.Unsetenv(MaintenanceEnvKey)
	v := &Validator{}
	LoadMaintenance(v)
	if len(v.Missing()) != 1 || !strings.HasPrefix(v.Missing()[0], MaintenanceEnvKey) {
		t.Fatalf("want one problem naming %s, got %v", MaintenanceEnvKey, v.Missing())
	}
}
