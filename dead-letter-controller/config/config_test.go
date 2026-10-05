package config

import (
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// setRequiredEnv sets every variable Load requires, so the validator reports
// nothing missing and a test can vary one setting at a time.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_DB", "continuo_dead_letter")
	t.Setenv("POSTGRES_USER", "continuo")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("METRICS_PORT", "9464")
}

func TestValidate_StreamRetentionBounds(t *testing.T) {
	ok := Config{TrimEnabled: true, StreamRetention: 72 * time.Hour}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, r := range []time.Duration{0, -time.Hour, model.ReplayHorizon + time.Second} {
		if err := (Config{TrimEnabled: true, StreamRetention: r}).Validate(); err == nil {
			t.Errorf("retention %v accepted", r)
		}
	}
	// An entry quarantined at the horizon is already past its redrive window.
	if err := (Config{TrimEnabled: true, StreamRetention: model.ReplayHorizon}).Validate(); err == nil {
		t.Error("the horizon itself must be rejected")
	}
	if err := (Config{TrimEnabled: true, StreamRetention: model.ReplayHorizon - time.Second}).Validate(); err != nil {
		t.Errorf("a retention just under the horizon must be accepted: %v", err)
	}
}

func TestRequireTrimPoolCapacity(t *testing.T) {
	cases := []struct {
		name    string
		trim    bool
		open    int
		wantErr bool
	}{
		{"trim on, one connection", true, 1, true},
		{"trim on, two connections", true, 2, false},
		{"trim on, default pool", true, 12, false},
		{"trim off, one connection", false, 1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := RequireTrimPoolCapacity(c.trim, c.open); (err != nil) != c.wantErr {
				t.Fatalf("RequireTrimPoolCapacity(%v, %d) = %v, wantErr %v", c.trim, c.open, err, c.wantErr)
			}
		})
	}
}

func TestLoad_TrimDefaults(t *testing.T) {
	setRequiredEnv(t)
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("missing = %v", v.Missing())
	}
	if !cfg.TrimEnabled || cfg.StreamRetention != 72*time.Hour {
		t.Fatalf("cfg = %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoad_TrimSettings(t *testing.T) {
	t.Setenv("STREAM_TRIM_ENABLED", "false")
	t.Setenv("STREAM_RETENTION", "48h")
	setRequiredEnv(t)
	cfg := Load(&pkgconfig.Validator{})
	if cfg.TrimEnabled || cfg.StreamRetention != 48*time.Hour || cfg.ConfigErr != nil {
		t.Fatalf("cfg = %+v", cfg)
	}
	t.Setenv("STREAM_TRIM_ENABLED", "maybe")
	bad := Load(&pkgconfig.Validator{})
	if bad.ConfigErr == nil || bad.Validate() == nil {
		t.Fatal("an unparseable STREAM_TRIM_ENABLED must fail closed")
	}
	t.Setenv("STREAM_TRIM_ENABLED", "true")
	t.Setenv("STREAM_RETENTION", "three days")
	if Load(&pkgconfig.Validator{}).Validate() == nil {
		t.Fatal("an unparseable STREAM_RETENTION must fail closed")
	}
}
