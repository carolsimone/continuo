package deploycheck

import (
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// composeFile is the part of docker-compose.yml the NO_PROXY checks read: each
// service's environment, written either as a list of KEY=VALUE strings or as a
// mapping.
type composeFile struct {
	Services map[string]struct {
		Environment composeEnvironment `yaml:"environment"`
	} `yaml:"services"`
}

// composeEnvironment is a service's environment as a name -> value map,
// whichever of the two compose syntaxes the file used.
type composeEnvironment map[string]string

func (e *composeEnvironment) UnmarshalYAML(node *yaml.Node) error {
	env := composeEnvironment{}
	switch node.Kind {
	case yaml.SequenceNode:
		for _, item := range node.Content {
			name, value, _ := strings.Cut(item.Value, "=")
			env[name] = value
		}
	case yaml.MappingNode:
		var raw map[string]string
		if err := node.Decode(&raw); err != nil {
			return err
		}
		env = raw
	}
	*e = env
	return nil
}

// noProxyEntries returns the comma-separated hosts of a service's NO_PROXY.
func noProxyEntries(env composeEnvironment) []string {
	var entries []string
	for _, entry := range strings.Split(env["NO_PROXY"], ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			entries = append(entries, entry)
		}
	}
	return entries
}

// agent-chat runs the continuo CLI as a subprocess that inherits its
// HTTP_PROXY, and the CLI dials run-controller over gRPC. Unless the
// container's literal NO_PROXY names the compose service, that dial goes
// through egress-proxy, which refuses it, and every chat tool call fails in
// compose while working in Kubernetes. The compose stack has no service named
// state, so no NO_PROXY may list that host either.
func TestComposeNoProxyNamesRunController(t *testing.T) {
	var compose composeFile
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "docker-compose.yml")), &compose); err != nil {
		t.Fatalf("parse docker-compose.yml: %v", err)
	}

	chat, ok := compose.Services["agent-chat"]
	if !ok {
		t.Fatal("docker-compose.yml has no agent-chat service")
	}
	if !slices.Contains(noProxyEntries(chat.Environment), "run-controller") {
		t.Errorf("agent-chat's NO_PROXY = %q, want an entry run-controller", chat.Environment["NO_PROXY"])
	}

	for name, svc := range compose.Services {
		if slices.Contains(noProxyEntries(svc.Environment), "state") {
			t.Errorf("service %q has NO_PROXY = %q: no compose service is named state", name, svc.Environment["NO_PROXY"])
		}
	}
}

// agent-chat's CLI subprocess also dials the catalog service over gRPC, so its
// literal NO_PROXY must name the compose service catalog. No NO_PROXY may list
// the host orchestrator: no compose service is named that.
func TestComposeNoProxyNamesCatalog(t *testing.T) {
	var compose composeFile
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "docker-compose.yml")), &compose); err != nil {
		t.Fatalf("parse docker-compose.yml: %v", err)
	}

	chat, ok := compose.Services["agent-chat"]
	if !ok {
		t.Fatal("docker-compose.yml has no agent-chat service")
	}
	if !slices.Contains(noProxyEntries(chat.Environment), "catalog") {
		t.Errorf("agent-chat's NO_PROXY = %q, want an entry catalog", chat.Environment["NO_PROXY"])
	}

	for name, svc := range compose.Services {
		if slices.Contains(noProxyEntries(svc.Environment), "orchestrator") {
			t.Errorf("service %q has NO_PROXY = %q: no compose service is named orchestrator", name, svc.Environment["NO_PROXY"])
		}
	}
}
