package config

import (
	"os"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

func TestLoad_MaxConcurrentJobsDefault(t *testing.T) {
	os.Setenv("K8S_NAMESPACE", "default")
	os.Unsetenv("MAX_CONCURRENT_JOBS")
	defer func() { os.Unsetenv("K8S_NAMESPACE") }()

	cfg := Load(&pkgconfig.Validator{})
	if cfg.MaxConcurrentJobs != 50 {
		t.Fatalf("want default 50, got %d", cfg.MaxConcurrentJobs)
	}
}

func TestLoad_MaxConcurrentJobsFromEnv(t *testing.T) {
	os.Setenv("K8S_NAMESPACE", "default")
	os.Setenv("MAX_CONCURRENT_JOBS", "12")
	defer func() {
		os.Unsetenv("K8S_NAMESPACE")
		os.Unsetenv("MAX_CONCURRENT_JOBS")
	}()

	cfg := Load(&pkgconfig.Validator{})
	if cfg.MaxConcurrentJobs != 12 {
		t.Fatalf("want 12, got %d", cfg.MaxConcurrentJobs)
	}
}
