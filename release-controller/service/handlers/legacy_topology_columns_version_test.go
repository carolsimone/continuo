package handlers

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestUpgradeThroughReleaseMatchesTheMigration pins upgradeThroughRelease to the
// version release migration V26 names in its refusal. The startup check and the
// migration both tell an operator to upgrade through a release before this one;
// if they named different versions the operator would get contradictory advice.
func TestUpgradeThroughReleaseMatchesTheMigration(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	matches, err := filepath.Glob(filepath.Join(repoRoot, "db", "migration", "release", "V26__*.sql"))
	if err != nil {
		t.Fatalf("glob V26 migration: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one V26 migration, found %d", len(matches))
	}
	sql, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read V26 migration: %v", err)
	}

	want := "upgrade to continuo " + upgradeThroughRelease + " and let release-controller start once before upgrading to this release"
	if !strings.Contains(string(sql), want) {
		t.Fatalf("release migration V26 does not carry %q; the startup check and the migration disagree on the upgrade-through version", want)
	}
}
