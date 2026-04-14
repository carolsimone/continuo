package config

import "testing"

func TestLoadPostgres_records_all_missing(t *testing.T) {
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"} {
		t.Setenv(key, "")
	}
	v := &Validator{}
	LoadPostgres(v)
	if got := len(v.Missing()); got != 4 {
		t.Fatalf("want 4 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadPostgres_no_missing_when_all_set(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "dbhost")
	t.Setenv("POSTGRES_DB", "mydb")
	t.Setenv("POSTGRES_USER", "usr")
	t.Setenv("POSTGRES_PASSWORD", "pw")
	v := &Validator{}
	cfg := LoadPostgres(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "dbhost" || cfg.DB != "mydb" || cfg.User != "usr" || cfg.Password != "pw" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
