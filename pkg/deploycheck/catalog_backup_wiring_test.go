package deploycheck

import (
	"regexp"
	"strings"
	"testing"
)

// catalogBackupKeys are the environment keys the chart's catalog.backup.*
// values reach catalog through.
var catalogBackupKeys = []string{"CATALOG_BACKUP_ENABLED", "CATALOG_BACKUP_SCHEDULE", "CATALOG_BACKUP_KEEP"}

// The backup settings reach catalog on every deployment path, and catalog's
// configuration reads each of them: the chart's shared ConfigMap, the
// catalog service in compose, and catalog/config/config.go.
func TestCatalogBackupKeysMatchTheChart(t *testing.T) {
	cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml")
	compose := readRepoFile(t, "docker-compose.yml")
	block := regexp.MustCompile(`(?ms)^  catalog:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
	if block == nil {
		t.Fatal("docker-compose.yml has no catalog service")
	}
	cfg := readRepoFile(t, "catalog", "config", "config.go")
	for _, key := range catalogBackupKeys {
		if !strings.Contains(cm, "\n  "+key+":") {
			t.Errorf("templates/configmap.yaml does not set %s", key)
		}
		if !strings.Contains(block[1], "- "+key+"=") {
			t.Errorf("docker-compose.yml service catalog does not set %s", key)
		}
		if !strings.Contains(cfg, `"`+key+`"`) {
			t.Errorf("catalog/config/config.go does not read %s", key)
		}
	}
}
