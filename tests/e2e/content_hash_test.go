package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"testing"
)

// sha256Hex is an independent, deliberately explicit re-implementation of the
// hashing primitive, kept separate from macroAwareContentHash so this test pins
// the exact byte layout the changed-node derivation depends on rather than
// echoing the code under test.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestMacroAwareContentHash_MatchesManifestControllerAlgorithm locks in the
// harness fingerprint so it keeps matching manifest-controller's _content_hash.
// The ReleasePromote tests seed current_prod with this value; if it drifts from
// the folded hash the candidate parse produces, every macro-dependent node
// (an incremental model reaches is_incremental) is derived as perpetually
// changed and drags its downstream into validation. Each case recomputes the
// expected hash independently, pinning: base-first concatenation, ascending
// macro-hash order joined with no separator, the "sha256:" prefix on the fold,
// the transitive macro->macro walk, and the skip of ids with no macro body.
func TestMacroAwareContentHash_MatchesManifestControllerAlgorithm(t *testing.T) {
	// A macro graph with one macro->macro edge (a -> b), one unreferenced macro
	// (c) that must never fold in, and — in the last case — a dependency id that
	// has no body in the manifest.
	macroDeps := map[string][]string{
		"macro.pkg.a": {"macro.pkg.b"},
		"macro.pkg.b": nil,
		"macro.pkg.c": nil,
	}
	macroSQL := map[string]string{
		"macro.pkg.a": "AAA",
		"macro.pkg.b": "BBB",
		"macro.pkg.c": "CCC",
	}

	t.Run("no macro dependencies returns the base unchanged", func(t *testing.T) {
		if got := macroAwareContentHash("rawchecksum", nil, macroDeps, macroSQL); got != "rawchecksum" {
			t.Fatalf("want base unchanged, got %q", got)
		}
	})

	t.Run("folds sorted macro-source hashes over the transitive closure", func(t *testing.T) {
		base := "deadbeef"
		// Transitive closure of a is {a, b}; c is unreferenced and must not appear.
		hashes := []string{sha256Hex("AAA"), sha256Hex("BBB")}
		sort.Strings(hashes)
		want := "sha256:" + sha256Hex(base+strings.Join(hashes, ""))

		if got := macroAwareContentHash(base, []string{"macro.pkg.a"}, macroDeps, macroSQL); got != want {
			t.Fatalf("folded hash mismatch:\n want %s\n  got %s", want, got)
		}
	})

	t.Run("dependency ids absent from the macro map contribute nothing", func(t *testing.T) {
		base := "deadbeef"
		hashes := []string{sha256Hex("AAA"), sha256Hex("BBB")}
		sort.Strings(hashes)
		want := "sha256:" + sha256Hex(base+strings.Join(hashes, ""))

		// macro.pkg.missing is reached but has no body, so it stays in the
		// closure yet folds nothing — exactly as the Python's `if mid in macros`
		// guard skips it.
		got := macroAwareContentHash(base, []string{"macro.pkg.a", "macro.pkg.missing"}, macroDeps, macroSQL)
		if got != want {
			t.Fatalf("missing-macro handling mismatch:\n want %s\n  got %s", want, got)
		}
	})
}
