package deploycheck

import (
	"encoding/json"
	"regexp"
	"slices"
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
