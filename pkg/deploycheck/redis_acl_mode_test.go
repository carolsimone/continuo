package deploycheck

import (
	"encoding/json"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// TestRedisACLModesMatchTheChart pins the REDIS_ACL_MODE values pkg/config
// accepts to credentials.redis.mode in the chart's values schema, and to the
// list the chart's continuo.redis.mode helper renders, so the chart cannot
// accept a mode the Go services refuse at start (or the reverse).
// topology-controller (tests/test_fence.py) and ui
// (tests/auth/redis-fence.test.ts) pin their own lists to the same schema.
func TestRedisACLModesMatchTheChart(t *testing.T) {
	var schema struct {
		Properties struct {
			Credentials struct {
				Properties struct {
					Redis struct {
						Properties struct {
							Mode struct {
								Enum []string `json:"enum"`
							} `json:"mode"`
						} `json:"properties"`
					} `json:"redis"`
				} `json:"properties"`
			} `json:"credentials"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.schema.json")), &schema); err != nil {
		t.Fatalf("parse values.schema.json: %v", err)
	}
	got := slices.Sorted(slices.Values(schema.Properties.Credentials.Properties.Redis.Properties.Mode.Enum))
	want := slices.Sorted(slices.Values([]string{pkgconfig.RedisACLModeShared, pkgconfig.RedisACLModePerService}))
	if !slices.Equal(got, want) {
		t.Fatalf("values.schema.json credentials.redis.mode enum = %v, pkg/config accepts %v", got, want)
	}
	var helper []string
	helperModes(t, &helper)
	if !slices.Equal(slices.Sorted(slices.Values(helper)), want) {
		t.Fatalf("_helpers.tpl continuo.redis.mode accepts %v, pkg/config %v", helper, want)
	}
}

// helperModes reads the values the chart's continuo.redis.mode helper accepts
// from its `has $mode (list ...)` check.
func helperModes(t *testing.T, into *[]string) {
	t.Helper()
	helpers := readRepoFile(t, "deploy", "continuo", "templates", "_helpers.tpl")
	m := regexp.MustCompile(`(?s)define "continuo\.redis\.mode".*?has \$mode \(list ((?:"[^"]*"\s*)+)\)`).FindStringSubmatch(helpers)
	if m == nil {
		t.Fatal(`_helpers.tpl continuo.redis.mode has no "has $mode (list ...)" check`)
	}
	for _, q := range regexp.MustCompile(`"([^"]*)"`).FindAllStringSubmatch(m[1], -1) {
		*into = append(*into, q[1])
	}
}

// quotedValues returns every quoted string literal the first match of pattern
// in the file at path captures.
func quotedValues(t *testing.T, pattern string, path ...string) []string {
	t.Helper()
	m := regexp.MustCompile(pattern).FindStringSubmatch(readRepoFile(t, path...))
	if m == nil {
		t.Fatalf("%s: no match for %s", filepath.Join(path...), pattern)
	}
	var out []string
	for _, q := range regexp.MustCompile(`["']([^"']*)["']`).FindAllStringSubmatch(m[1], -1) {
		out = append(out, q[1])
	}
	return slices.Sorted(slices.Values(out))
}

// TestRedisACLModesMatchThePythonAndNodeLists pins the REDIS_ACL_MODE values
// topology-controller (its config and its fence) and ui accept to the ones
// pkg/config accepts, which TestRedisACLModesMatchTheChart pins to the chart.
// The check reads their sources here because their own test suites run in
// images that hold no chart.
func TestRedisACLModesMatchThePythonAndNodeLists(t *testing.T) {
	want := slices.Sorted(slices.Values([]string{pkgconfig.RedisACLModeShared, pkgconfig.RedisACLModePerService}))
	for _, side := range []struct {
		where   string
		pattern string
		path    []string
	}{
		{"topology-controller config", `(?s)(REDIS_ACL_MODE_SHARED = "[^"]*"\s*REDIS_ACL_MODE_PER_SERVICE = "[^"]*")`, []string{"topology-controller", "config", "config.py"}},
		{"topology-controller fence", `(?s)(SHARED = "[^"]*"\s*PER_SERVICE = "[^"]*")`, []string{"topology-controller", "adapters", "redis", "fence.py"}},
		{"ui", `REDIS_ACL_MODES = \[([^\]]*)\]`, []string{"ui", "src", "server", "auth", "redis-fence.ts"}},
	} {
		if got := quotedValues(t, side.pattern, side.path...); !slices.Equal(got, want) {
			t.Errorf("%s accepts REDIS_ACL_MODE %v, pkg/config %v", side.where, got, want)
		}
	}
}

// TestPermissionProbeKeysMatchAndLieOutsideEveryRule: Go, topology-controller
// and ui probe the same key, and no generated login's key pattern covers it,
// or that login's own fence would refuse to start its service.
func TestPermissionProbeKeysMatchAndLieOutsideEveryRule(t *testing.T) {
	keys := map[string][]string{
		"pkg/redis":           quotedValues(t, `permissionProbeKey = ("[^"]*")`, "pkg", "redis", "fence.go"),
		"topology-controller": quotedValues(t, `PERMISSION_PROBE_KEY = ("[^"]*")`, "topology-controller", "adapters", "redis", "fence.py"),
		"ui":                  quotedValues(t, `PERMISSION_PROBE_KEY = ('[^']*')`, "ui", "src", "server", "auth", "redis-fence.ts"),
	}
	probe := keys["pkg/redis"][0]
	for where, k := range keys {
		if len(k) != 1 || k[0] != probe {
			t.Errorf("%s probes %v, pkg/redis %q", where, k, probe)
		}
	}
	var rules struct {
		Logins []struct {
			Username string `json:"username"`
			Rule     string `json:"rule"`
		} `json:"logins"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "files", "redis-acl-rules.json")), &rules); err != nil {
		t.Fatalf("parse redis-acl-rules.json: %v", err)
	}
	for _, l := range rules.Logins {
		for _, token := range strings.Fields(l.Rule) {
			_, pattern, ok := strings.Cut(token, "~")
			if !ok {
				continue
			}
			if covered, err := path.Match(pattern, probe); err != nil || covered {
				t.Errorf("%s's key pattern %q covers the probe key %q (err %v)", l.Username, pattern, probe, err)
			}
		}
	}
}
