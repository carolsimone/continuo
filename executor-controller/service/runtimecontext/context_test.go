package runtimecontext

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// envMap turns a fixed environment into a getenv func; unset keys read empty,
// exactly as os.Getenv would.
func envMap(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

func fullEnv() map[string]string {
	return map[string]string{
		"DBT_TARGET":        "prod",
		"DBT_POSTGRES_DB":   "continuo",
		"DBT_TARGET_SCHEMA": "analytics",
		"DBT_PROFILES_DIR":  "/project",
		"DBT_PROJECT_DIR":   "/project",
	}
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// The context is hashed into a digest that decides whether a cached parse may
// be reused, so identical inputs must serialize byte-identically every time.
func TestBuild_Deterministic(t *testing.T) {
	first, err := Build("command-context", envMap(fullEnv()))
	require.NoError(t, err)
	require.NotEmpty(t, first)

	for i := 0; i < 20; i++ {
		got, err := Build("command-context", envMap(fullEnv()))
		require.NoError(t, err)
		assert.Equal(t, first, got, "identical inputs must give byte-identical JSON")
	}
}

func TestBuild_Shape(t *testing.T) {
	body, err := Build("command-context", envMap(fullEnv()))
	require.NoError(t, err)

	var got Context
	require.NoError(t, json.Unmarshal([]byte(body), &got))

	assert.Equal(t, digest("command-context"), got.CommandDialectSHA256)
	assert.Equal(t, Target{
		Name: "prod", Type: "postgres", Database: "continuo", Schema: "analytics",
	}, got.Target)
	assert.Equal(t, map[string]string{
		"DBT_TARGET":        digest("prod"),
		"DBT_POSTGRES_DB":   digest("continuo"),
		"DBT_TARGET_SCHEMA": digest("analytics"),
		"DBT_PROFILES_DIR":  digest("/project"),
		"DBT_PROJECT_DIR":   digest("/project"),
	}, got.EnvironmentSHA256, "every declared key is recorded as a digest of its value")
}

// An unset DBT_TARGET means the dbt profile's own default target, which is dev.
func TestBuild_DefaultsTargetNameToDev(t *testing.T) {
	env := fullEnv()
	delete(env, "DBT_TARGET")
	body, err := Build("command-context", envMap(env))
	require.NoError(t, err)

	var got Context
	require.NoError(t, json.Unmarshal([]byte(body), &got))
	assert.Equal(t, "dev", got.Target.Name)
	assert.Equal(t, digest(""), got.EnvironmentSHA256["DBT_TARGET"],
		"the digest records the raw unset value, not the substituted default")
}

// Anything that changes what dbt would parse must change the context, so a
// stale artifact can never be mistaken for a matching one.
func TestBuild_ChangesWithCommandContextOrEnv(t *testing.T) {
	base, err := Build("command-context", envMap(fullEnv()))
	require.NoError(t, err)

	changedCommand, err := Build("other-command-context", envMap(fullEnv()))
	require.NoError(t, err)
	assert.NotEqual(t, base, changedCommand, "a changed command dialect changes the context")

	for _, key := range []string{
		"DBT_TARGET", "DBT_POSTGRES_DB", "DBT_TARGET_SCHEMA", "DBT_PROFILES_DIR", "DBT_PROJECT_DIR",
	} {
		t.Run("changed "+key, func(t *testing.T) {
			env := fullEnv()
			env[key] = env[key] + "-changed"
			got, err := Build("command-context", envMap(env))
			require.NoError(t, err)
			assert.NotEqual(t, base, got, "a changed %s changes the context", key)
		})
	}
}

// The context is persisted and shipped between services, so it must carry
// digests of the environment, never the environment itself.
func TestBuild_NeverEmitsRawSecrets(t *testing.T) {
	const password = "sup3r-s3cret-pw" //nolint:gosec // G101: test fixture, not a real credential.
	env := fullEnv()
	env["DBT_PASSWORD"] = password
	env["DBT_POSTGRES_PASSWORD"] = password
	env["PGPASSWORD"] = password

	body, err := Build("command-context", envMap(env))
	require.NoError(t, err)
	assert.NotContains(t, body, password, "a password env value must never reach the context")
	assert.NotContains(t, body, "PASSWORD", "password env keys are not part of the context")
}

// Build reads exactly the declared keys, which is what keeps an unrelated
// secret in the process environment out of the context by construction.
func TestBuild_ReadsOnlyDeclaredKeys(t *testing.T) {
	read := map[string]bool{}
	getenv := func(key string) string {
		read[key] = true
		return fullEnv()[key]
	}
	_, err := Build("command-context", getenv)
	require.NoError(t, err)

	keys := make([]string, 0, len(read))
	for key := range read {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{
		"DBT_POSTGRES_DB", "DBT_PROFILES_DIR", "DBT_PROJECT_DIR", "DBT_TARGET", "DBT_TARGET_SCHEMA",
	}, keys)
}

// The hashed values are opaque, so a value that is itself sensitive appears
// only as its digest.
func TestBuild_EnvValuesAppearOnlyAsDigests(t *testing.T) {
	env := fullEnv()
	env["DBT_PROFILES_DIR"] = "/secrets/tenant-42"

	body, err := Build("command-context", envMap(env))
	require.NoError(t, err)

	assert.NotContains(t, body, "/secrets/tenant-42")
	assert.Contains(t, body, digest("/secrets/tenant-42"))
	assert.True(t, strings.Contains(body, `"environment_sha256"`))
}
