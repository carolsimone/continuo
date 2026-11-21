package deploycheck

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// composeServiceBlock returns the text of one service of docker-compose.yml.
func composeServiceBlock(t *testing.T, service string) string {
	t.Helper()
	compose := readRepoFile(t, "docker-compose.yml")
	block := regexp.MustCompile(`(?ms)^  ` + regexp.QuoteMeta(service) + `:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
	if block == nil {
		t.Fatalf("docker-compose.yml has no %s service", service)
	}
	return block[1]
}

// The ontology settings reach every process that acts on them, on every
// deployment path that runs it, and each service's configuration reads the
// key it is given:
//   - ONTOLOGY_ENABLED (contracts.ontology.enabled) is read by
//     topology-controller and release-controller. Compose sets it to false on
//     both: topology-controller's environment is a map, release-controller's a
//     list.
//   - ONTOLOGY_BASE_URL (contracts.ontology.baseUrl) is read by catalog.
//     Compose sets it, empty, on catalog.
//
// Both keys are on the chart's shared ConfigMap.
func TestOntologyKeysMatchTheChart(t *testing.T) {
	cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml")
	for _, key := range []string{"ONTOLOGY_ENABLED", "ONTOLOGY_BASE_URL"} {
		if !strings.Contains(cm, "\n  "+key+":") {
			t.Errorf("templates/configmap.yaml does not set %s", key)
		}
	}

	if block := composeServiceBlock(t, "topology-controller"); !strings.Contains(block, "\n      ONTOLOGY_ENABLED: \"false\"\n") {
		t.Errorf(`docker-compose.yml service topology-controller does not set ONTOLOGY_ENABLED: "false"`)
	}
	if block := composeServiceBlock(t, "release-controller"); !strings.Contains(block, "\n      - ONTOLOGY_ENABLED=false\n") {
		t.Errorf("docker-compose.yml service release-controller does not set ONTOLOGY_ENABLED=false")
	}
	if block := composeServiceBlock(t, "catalog"); !strings.Contains(block, "\n      - ONTOLOGY_BASE_URL=\n") {
		t.Errorf("docker-compose.yml service catalog does not set ONTOLOGY_BASE_URL to the empty value")
	}

	for _, reader := range []struct {
		path []string
		key  string
	}{
		{[]string{"topology-controller", "config", "config.py"}, "ONTOLOGY_ENABLED"},
		{[]string{"release-controller", "config", "config.go"}, "ONTOLOGY_ENABLED"},
		{[]string{"catalog", "config", "config.go"}, "ONTOLOGY_BASE_URL"},
	} {
		if !strings.Contains(readRepoFile(t, reader.path...), `"`+reader.key+`"`) {
			t.Errorf("%s does not read %s", strings.Join(reader.path, "/"), reader.key)
		}
	}
}

// contracts.ontology.baseUrl is checked by the chart's values schema and again
// by catalog at boot. The schema's pattern accepts nothing catalog refuses, so
// a value that renders never stops catalog: an empty value, or an absolute
// http or https URL with a host, no query, no fragment and no whitespace. The
// values catalog accepts and refuses in its own tests are held to that rule
// below. The pattern is stricter than catalog on four values catalog accepts:
// a space in the path, credentials in the URL, an empty port and a host written
// as an IPv6 literal.
func TestOntologyBaseURLSchemaPatternIsAsStrictAsCatalog(t *testing.T) {
	var schema struct {
		Properties struct {
			Contracts struct {
				Properties struct {
					Ontology struct {
						Properties struct {
							BaseURL struct {
								Pattern string `json:"pattern"`
							} `json:"baseUrl"`
						} `json:"properties"`
					} `json:"ontology"`
				} `json:"properties"`
			} `json:"contracts"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.schema.json")), &schema); err != nil {
		t.Fatalf("values.schema.json: %v", err)
	}
	pattern := schema.Properties.Contracts.Properties.Ontology.Properties.BaseURL.Pattern
	if pattern == "" {
		t.Fatal("values.schema.json has no pattern for contracts.ontology.baseUrl")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("contracts.ontology.baseUrl pattern %q: %v", pattern, err)
	}

	for _, raw := range []string{
		"",
		"https://ontology.example",
		"https://ontology.example/",
		"https://ontology.example/things",
		"https://x/things/",
		"http://ontology.internal:8080/things",
		"https://x//",
	} {
		if !re.MatchString(raw) {
			t.Errorf("pattern refuses %q, which catalog accepts", raw)
		}
	}
	for _, raw := range []string{
		"ftp://x", "https://", "http://", "x/y", "/things", "ontology.example", "https://x?q=1", "https://x/things?",
		"https://x#section", "https://x/things#", "https:///things", "mailto:a@b.c", "https://x y", "https://x/a b",
		" https://x", "https://x ", "https://x/\tthings", "https://x/things\n", "https://x:port/", "https://x/%zz",
		"https://user:secret@x/things", "https://x:/things", "http://[::1]:8080/things",
	} {
		if re.MatchString(raw) {
			t.Errorf("pattern accepts %q, which the chart must refuse", raw)
		}
	}
}
