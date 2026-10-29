package streams_test

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestConsumerGroupsStartWithTheirServiceName pins the naming rule of a
// consumer group: it is `<consuming service>-<purpose>`, so the group a Redis
// stream shows in XINFO GROUPS names the service that reads through it.
func TestConsumerGroupsStartWithTheirServiceName(t *testing.T) {
	raw, err := os.ReadFile("contract.yaml")
	if err != nil {
		t.Fatalf("read contract.yaml: %v", err)
	}
	var c struct {
		Streams []struct {
			Name      string `yaml:"name"`
			Consumers []struct {
				Service string `yaml:"service"`
				Group   string `yaml:"group"`
			} `yaml:"consumers"`
		} `yaml:"streams"`
	}
	if err := yaml.Unmarshal(raw, &c); err != nil {
		t.Fatalf("parse contract.yaml: %v", err)
	}

	checked := 0
	for _, s := range c.Streams {
		for _, cons := range s.Consumers {
			checked++
			if !strings.HasPrefix(cons.Group, cons.Service+"-") {
				t.Errorf("group %q of service %q must start with %q", cons.Group, cons.Service, cons.Service+"-")
			}
		}
	}
	if checked == 0 {
		t.Fatal("contract.yaml declares no consumer groups: the guard is not reading the contract")
	}
}
