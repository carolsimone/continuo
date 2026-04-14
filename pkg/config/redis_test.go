package config

import "testing"

func TestLoadRedis_records_all_missing(t *testing.T) {
	for _, key := range []string{"REDIS_HOST", "REDIS_PORT", "REDIS_PASSWORD"} {
		t.Setenv(key, "")
	}
	v := &Validator{}
	LoadRedis(v)
	if got := len(v.Missing()); got != 3 {
		t.Fatalf("want 3 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadRedis_no_missing_when_all_set(t *testing.T) {
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedis(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "redishost" || cfg.Port != 6380 || cfg.Password != "secret" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadRedisFromAddr_records_all_missing(t *testing.T) {
	for _, key := range []string{"REDIS_ADDR", "REDIS_PASSWORD"} {
		t.Setenv(key, "")
	}
	v := &Validator{}
	LoadRedisFromAddr(v)
	if got := len(v.Missing()); got != 2 {
		t.Fatalf("want 2 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadRedisFromAddr_no_missing_when_all_set(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redishost:6380")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedisFromAddr(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "redishost" || cfg.Port != 6380 || cfg.Password != "secret" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
