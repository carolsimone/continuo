// Package runtimecontext builds the canonical parse context: the controller's
// description of everything that decides what a dbt parse would produce for a
// service. Hashing it yields a digest that answers one question — may a
// previously produced parse artifact be reused, or was it made under different
// conditions and must be rebuilt?
package runtimecontext

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// contextKeys are the environment variables that change what dbt parses. Build
// reads these and nothing else, which is what keeps unrelated secrets in the
// process environment out of the context by construction.
var contextKeys = []string{
	"DBT_TARGET",
	"DBT_POSTGRES_DB",
	"DBT_TARGET_SCHEMA",
	"DBT_PROFILES_DIR",
	"DBT_PROJECT_DIR",
}

// defaultTargetName is the target dbt profiles fall back to when DBT_TARGET is
// unset.
const defaultTargetName = "dev"

// Context is the canonical parse context. It is marshaled as a struct so its
// field order is fixed and the JSON is byte-stable across processes.
type Context struct {
	// CommandDialectSHA256 digests the service's resolved command surface.
	CommandDialectSHA256 string `json:"command_dialect_sha256"`
	// Target identifies the warehouse the parse resolves against.
	Target Target `json:"target"`
	// EnvironmentSHA256 maps each context key to a digest of its value.
	// Values are hashed, never carried, so the context stays safe to
	// persist and ship between services.
	EnvironmentSHA256 map[string]string `json:"environment_sha256"`
}

// Target is the dbt target a parse resolves against.
type Target struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
}

// Build returns the canonical JSON parse context for a service whose resolved
// command surface is commandContext, reading the environment through getenv.
func Build(commandContext string, getenv func(string) string) (string, error) {
	commandSum := sha256.Sum256([]byte(commandContext))

	envHashes := make(map[string]string, len(contextKeys))
	for _, key := range contextKeys {
		valueSum := sha256.Sum256([]byte(getenv(key)))
		envHashes[key] = hex.EncodeToString(valueSum[:])
	}

	targetName := getenv("DBT_TARGET")
	if targetName == "" {
		targetName = defaultTargetName
	}

	body, err := json.Marshal(Context{
		CommandDialectSHA256: hex.EncodeToString(commandSum[:]),
		Target: Target{
			Name:     targetName,
			Type:     "postgres",
			Database: getenv("DBT_POSTGRES_DB"),
			Schema:   getenv("DBT_TARGET_SCHEMA"),
		},
		EnvironmentSHA256: envHashes,
	})
	if err != nil {
		return "", err
	}
	return string(body), nil
}
