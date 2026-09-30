// Package testdeps lets integration tests that need live infrastructure
// (Postgres, Neo4j, Redis) decide between skipping and failing when that
// infrastructure is unreachable.
//
// A developer running `go test` on a bare checkout has no databases, so the
// tests skip. The shared test entrypoint (`make test-go`) brings the
// dependencies up itself and sets RequireEnv; under it a missing dependency is
// a broken environment, and a skip would report a green run in which nothing
// was exercised.
package testdeps

import (
	"os"
	"testing"
)

// RequireEnv names the environment variable that turns unreachable-dependency
// skips into failures. Any non-empty value other than "0" enables it.
const RequireEnv = "REQUIRE_TEST_DEPS"

// Required reports whether unreachable dependencies must fail the test.
func Required() bool {
	v := os.Getenv(RequireEnv)
	return v != "" && v != "0"
}

// Unavailable ends the test because a live dependency it needs cannot be
// reached (or is not configured). It fails when Required() is true and skips
// otherwise.
func Unavailable(t testing.TB, format string, args ...any) {
	t.Helper()
	if Required() {
		t.Fatalf("required test dependency unavailable ("+RequireEnv+" is set): "+format, args...)
		return
	}
	t.Skipf(format, args...)
}
