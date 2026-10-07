package deploycheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/maintenance"
)

// enforcingServices are the services that refuse new work in maintenance mode
// and refuse to start without MAINTENANCE_ENABLED.
var enforcingServices = []string{"state", "release-controller", "agent-remediation", "dead-letter-controller"}

func readRepoFile(t *testing.T, rel ...string) string {
	t.Helper()
	p := filepath.Join(append([]string{repoRoot(t)}, rel...)...)
	raw, err := os.ReadFile(p) //nolint:gosec // G304: p is repoRoot(t) joined with fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(raw)
}

// The key reaches every process that acts on it: the chart's shared ConfigMap,
// and in compose every enforcing container plus both ui containers.
func TestMaintenanceFlagReachesEveryDeploymentPath(t *testing.T) {
	if cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml"); !strings.Contains(cm, pkgconfig.MaintenanceEnvKey+":") {
		t.Errorf("templates/configmap.yaml does not set %s", pkgconfig.MaintenanceEnvKey)
	}
	compose := readRepoFile(t, "docker-compose.yml")
	for _, svc := range append([]string{"ui", "ui-auth"}, enforcingServices...) {
		block := regexp.MustCompile(`(?ms)^  ` + regexp.QuoteMeta(svc) + `:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
		if block == nil || !strings.Contains(block[1], pkgconfig.MaintenanceEnvKey+"=false") {
			t.Errorf("docker-compose.yml service %q does not set %s=false", svc, pkgconfig.MaintenanceEnvKey)
		}
	}
}

// Every enforcing service reads the flag through the strict loader.
func TestEnforcingServicesLoadMaintenanceStrictly(t *testing.T) {
	for _, svc := range enforcingServices {
		if !strings.Contains(readRepoFile(t, svc, "config", "config.go"), "pkgconfig.LoadMaintenance(v)") {
			t.Errorf("%s/config/config.go does not read MAINTENANCE_ENABLED through pkgconfig.LoadMaintenance", svc)
		}
	}
}

// The CLI and ui cannot import pkg, so they carry their own copy of the
// trailer key and message; this pins both copies to pkg/maintenance.
func TestClientCopiesMatchPkgMaintenance(t *testing.T) {
	cli := readRepoFile(t, "cli", "internal", "client", "maintenance.go")
	if !strings.Contains(cli, `"`+maintenance.TrailerKey+`"`) {
		t.Errorf("cli/internal/client/maintenance.go trailer key differs from %q", maintenance.TrailerKey)
	}
	ui := readRepoFile(t, "ui", "src", "server", "maintenance.ts")
	for _, lit := range []string{maintenance.TrailerKey, maintenance.Message, maintenance.Code} {
		if !strings.Contains(ui, `'`+lit+`'`) {
			t.Errorf("ui/src/server/maintenance.ts does not carry %q", lit)
		}
	}
}
