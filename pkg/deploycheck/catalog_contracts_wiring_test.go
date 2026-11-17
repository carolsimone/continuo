package deploycheck

import (
	"regexp"
	"strings"
	"testing"
)

// catalogContractsKeys are the environment keys the chart's
// contracts.publish.server.* and catalog.contractHistory.* values reach
// catalog through, and WAREHOUSE_ENGINE, which the servers entry's type
// derives from.
var catalogContractsKeys = []string{
	"WAREHOUSE_ENGINE", "CONTRACTS_SERVER_HOST", "CONTRACTS_SERVER_PORT", "CONTRACTS_SERVER_DATABASE",
	"CONTRACT_HISTORY_RETENTION_DAYS",
}

// The contracts settings reach catalog on every deployment path that runs it,
// and catalog's configuration reads each of them: the chart's shared
// ConfigMap, the catalog service in compose, and catalog/config/config.go.
func TestCatalogContractsKeysMatchTheChart(t *testing.T) {
	cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml")
	compose := readRepoFile(t, "docker-compose.yml")
	block := regexp.MustCompile(`(?ms)^  catalog:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
	if block == nil {
		t.Fatal("docker-compose.yml has no catalog service")
	}
	cfg := readRepoFile(t, "catalog", "config", "config.go")
	for _, key := range catalogContractsKeys {
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
