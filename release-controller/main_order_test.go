package main

import (
	"os"
	"strings"
	"testing"
)

// The current_prod backfill must run before the legacy-topology upgrade.
// UpgradeLegacyTopologies advances the release queue, and a queue advance that
// loads a legacy current_prod naming a release with no topology artifact fails
// and crash-loops the boot before the backfill could repair it. main.go
// therefore calls BackfillCurrentProdArtifact first, then
// UpgradeLegacyTopologies.
func TestStartupRunsBackfillBeforeUpgrade(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	body := string(src)
	backfill := strings.Index(body, "handlers.BackfillCurrentProdArtifact(")
	upgrade := strings.Index(body, "handlers.UpgradeLegacyTopologies(")
	if backfill < 0 || upgrade < 0 {
		t.Fatalf("expected both startup steps to be called in main.go (backfill=%d upgrade=%d)", backfill, upgrade)
	}
	if backfill > upgrade {
		t.Fatal("BackfillCurrentProdArtifact must be called before UpgradeLegacyTopologies in main.go: " +
			"the upgrade advances the queue, which crash-loops on a legacy current_prod the backfill has not yet repaired")
	}
}
