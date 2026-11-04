package config

import "testing"

// clearRedisEnv unsets every variable the Redis loaders read, so a test sees
// only the values it sets.
func clearRedisEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"REDIS_HOST", "REDIS_PORT", "REDIS_ADDR", "REDIS_USERNAME",
		"REDIS_PASSWORD", "REDIS_PASSWORD_FILE", "REDIS_ACL_MODE",
	} {
		t.Setenv(key, "")
	}
}

func redisPassword(t *testing.T, cfg RedisConfig) string {
	t.Helper()
	pw, err := cfg.Password.Value()
	if err != nil {
		t.Fatalf("Password.Value(): %v", err)
	}
	return pw
}

func TestLoadRedis_records_all_missing(t *testing.T) {
	clearRedisEnv(t)
	v := &Validator{}
	LoadRedis(v)
	if got := len(v.Missing()); got != 3 {
		t.Fatalf("want 3 missing vars, got %d: %v", got, v.Missing())
	}
	if !containsEntry(v.Missing(), "REDIS_PASSWORD") {
		t.Fatalf("missing = %v; want REDIS_PASSWORD", v.Missing())
	}
}

func TestLoadRedis_no_missing_when_all_set(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedis(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "redishost" || cfg.Port != 6380 || cfg.Username != "" {
		t.Fatalf("unexpected config: host=%q port=%d username=%q", cfg.Host, cfg.Port, cfg.Username)
	}
	if got := redisPassword(t, cfg); got != "secret" {
		t.Fatalf("password = %q; want secret", got)
	}
}

func TestLoadRedis_reads_the_username(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_USERNAME", "run-controller")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedis(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Username != "run-controller" {
		t.Fatalf("username = %q; want run-controller", cfg.Username)
	}
}

func TestLoadRedis_reads_the_password_file(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PASSWORD_FILE", writeSecretFile(t, "from-file\n"))
	v := &Validator{}
	cfg := LoadRedis(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if got := redisPassword(t, cfg); got != "from-file" {
		t.Fatalf("password = %q; want from-file", got)
	}
}

func TestLoadRedis_both_password_forms_are_reported(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_PASSWORD_FILE", writeSecretFile(t, "secret"))
	v := &Validator{}
	LoadRedis(v)
	want := "REDIS_PASSWORD: set REDIS_PASSWORD or REDIS_PASSWORD_FILE, not both"
	if !containsEntry(v.Missing(), want) {
		t.Fatalf("missing = %v; want entry %q", v.Missing(), want)
	}
}

func TestLoadRedisFromAddr_records_all_missing(t *testing.T) {
	clearRedisEnv(t)
	v := &Validator{}
	LoadRedisFromAddr(v)
	if got := len(v.Missing()); got != 2 {
		t.Fatalf("want 2 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadRedisFromAddr_no_missing_when_all_set(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost:6380")
	t.Setenv("REDIS_USERNAME", "catalog")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedisFromAddr(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Host != "redishost" || cfg.Port != 6380 || cfg.Username != "catalog" {
		t.Fatalf("unexpected config: host=%q port=%d username=%q", cfg.Host, cfg.Port, cfg.Username)
	}
	if got := redisPassword(t, cfg); got != "secret" {
		t.Fatalf("password = %q; want secret", got)
	}
}

func TestLoadRedisFromAddr_reads_the_password_file(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost:6379")
	t.Setenv("REDIS_PASSWORD_FILE", writeSecretFile(t, "from-file\r\n"))
	v := &Validator{}
	cfg := LoadRedisFromAddr(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if got := redisPassword(t, cfg); got != "from-file" {
		t.Fatalf("password = %q; want from-file", got)
	}
}

func TestLoadRedisFromAddr_no_port_defaults(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedisFromAddr(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing for host-only addr (uses default port), got %v", v.Missing())
	}
	if cfg.Host != "redishost" || cfg.Port != 6379 {
		t.Fatalf("unexpected config: host=%q port=%d", cfg.Host, cfg.Port)
	}
}

func TestLoadRedisFromAddr_empty_port_records_invalid(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost:")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	LoadRedisFromAddr(v)
	if !containsEntry(v.Missing(), "REDIS_ADDR") {
		t.Fatalf("want REDIS_ADDR in missing, got %v", v.Missing())
	}
}

func TestLoadOptionalRedis_unset_addr_is_not_configured(t *testing.T) {
	clearRedisEnv(t)
	v := &Validator{}
	_, ok := LoadOptionalRedis(v)
	if ok {
		t.Fatal("LoadOptionalRedis reported a Redis with REDIS_ADDR unset")
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing with REDIS_ADDR unset, got %v", v.Missing())
	}
}

func TestLoadOptionalRedis_set_addr_reads_the_connection(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost:6380")
	t.Setenv("REDIS_PASSWORD_FILE", writeSecretFile(t, "from-file"))
	v := &Validator{}
	cfg, ok := LoadOptionalRedis(v)
	if !ok {
		t.Fatal("LoadOptionalRedis reported no Redis with REDIS_ADDR set")
	}
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.Addr() != "redishost:6380" {
		t.Fatalf("addr = %q; want redishost:6380", cfg.Addr())
	}
	if got := redisPassword(t, cfg); got != "from-file" {
		t.Fatalf("password = %q; want from-file", got)
	}
}

func TestLoadOptionalRedis_set_addr_requires_the_password(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost:6379")
	v := &Validator{}
	if _, ok := LoadOptionalRedis(v); !ok {
		t.Fatal("LoadOptionalRedis reported no Redis with REDIS_ADDR set")
	}
	if !containsEntry(v.Missing(), "REDIS_PASSWORD") {
		t.Fatalf("missing = %v; want REDIS_PASSWORD", v.Missing())
	}
}

func TestLoadRedis_acl_mode_defaults_to_shared(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	v := &Validator{}
	cfg := LoadRedis(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.ACLMode != RedisACLModeShared {
		t.Fatalf("ACLMode = %q; want %q", cfg.ACLMode, RedisACLModeShared)
	}
}

func TestLoadRedisFromAddr_reads_the_acl_mode(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_ADDR", "redishost:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_ACL_MODE", "perService")
	v := &Validator{}
	cfg := LoadRedisFromAddr(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.ACLMode != RedisACLModePerService {
		t.Fatalf("ACLMode = %q; want %q", cfg.ACLMode, RedisACLModePerService)
	}
}

func TestLoadRedis_unknown_acl_mode_is_reported(t *testing.T) {
	clearRedisEnv(t)
	t.Setenv("REDIS_HOST", "redishost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_ACL_MODE", "perservice")
	v := &Validator{}
	LoadRedis(v)
	want := `REDIS_ACL_MODE (invalid value "perservice": expected shared or perService)`
	if !containsEntry(v.Missing(), want) {
		t.Fatalf("missing = %v; want %q", v.Missing(), want)
	}
}
