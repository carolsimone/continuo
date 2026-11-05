package main

import (
	_ "embed"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// aclFixtureYAML is a small contract: two services that talk over one stream,
// both dead-letter streams, one stream no service writes any more, and the
// plain-key and all-streams grants of ui and dead-letter-controller.
const aclFixtureYAML = `
key_prefix: "continuo:"
streams:
  - name: task.dispatch:v1
    const: TaskDispatchV1
    producers: [run-controller]
    consumers:
      - service: execution-controller
        group: execution-controller-task-dispatch
        const: ExecutionControllerTaskDispatch
  - name: consumer.dead_letter:v1
    const: ConsumerDeadLetterV1
    producers: [run-controller, execution-controller, dead-letter-controller]
    consumers:
      - service: dead-letter-controller
        group: dead-letter-controller-consumer-dead-letters
        const: DeadLetterControllerConsumerDeadLetters
  - name: outbox.dead_letter:v1
    const: OutboxDeadLetterV1
    producers: [run-controller, dead-letter-controller]
    consumers:
      - service: dead-letter-controller
        group: dead-letter-controller-outbox-dead-letters
        const: DeadLetterControllerOutboxDeadLetters
retired_streams:
  - name: task.failed:v1
redis_access:
  - service: ui
    keys: [uisession:]
    commands: [get, set, del, expire]
  - service: dead-letter-controller
    all_streams: true
    commands: [xrange, xtrim, xlen, del, info, xinfo|groups, xpending]
`

//go:embed testdata/golden-redis-acl-rules.json.txt
var goldenACLJSON string

//go:embed testdata/golden-redis-acl-rules.acl.txt
var goldenACLRules string

//go:embed testdata/golden-redis-users.acl.txt
var goldenComposeACL string

func mustLoad(t *testing.T, y string) *Contract {
	t.Helper()
	c, err := loadAndValidate(strings.NewReader(y))
	if err != nil {
		t.Fatalf("loadAndValidate: %v", err)
	}
	return c
}

func loadRealContract(t *testing.T) *Contract {
	t.Helper()
	f, err := os.Open("../../contract.yaml")
	if err != nil {
		t.Fatalf("open contract: %v", err)
	}
	defer func() { _ = f.Close() }()
	c, err := loadAndValidate(f)
	if err != nil {
		t.Fatalf("load contract: %v", err)
	}
	return c
}

// TestACLRulesGolden pins the three outputs: the machine-readable rules, the
// ACL file published with the chart, and the compose ACL file with local
// passwords.
func TestACLRulesGolden(t *testing.T) {
	c := mustLoad(t, aclFixtureYAML)

	gotJSON, err := emitRedisACLJSON(c)
	if err != nil {
		t.Fatalf("emitRedisACLJSON: %v", err)
	}
	if gotJSON != goldenACLJSON {
		t.Errorf("emitRedisACLJSON mismatch.\n--- got ---\n%s\n--- want ---\n%s", gotJSON, goldenACLJSON)
	}

	gotACL, err := emitRedisACL(c)
	if err != nil {
		t.Fatalf("emitRedisACL: %v", err)
	}
	if gotACL != goldenACLRules {
		t.Errorf("emitRedisACL mismatch.\n--- got ---\n%s\n--- want ---\n%s", gotACL, goldenACLRules)
	}

	gotCompose, err := emitRedisComposeACL(c)
	if err != nil {
		t.Fatalf("emitRedisComposeACL: %v", err)
	}
	if gotCompose != goldenComposeACL {
		t.Errorf("emitRedisComposeACL mismatch.\n--- got ---\n%s\n--- want ---\n%s", gotCompose, goldenComposeACL)
	}
}

// TestACLRulesAreDeterministic emits the rules twice and from a contract with
// its streams in the opposite order: the text does not change.
func TestACLRulesAreDeterministic(t *testing.T) {
	c := loadRealContract(t)
	first, err := emitRedisACLJSON(c)
	if err != nil {
		t.Fatalf("emitRedisACLJSON: %v", err)
	}
	slices.Reverse(c.Streams)
	for i := range c.Streams {
		slices.Reverse(c.Streams[i].Producers)
		slices.Reverse(c.Streams[i].Consumers)
	}
	second, err := emitRedisACLJSON(c)
	if err != nil {
		t.Fatalf("emitRedisACLJSON: %v", err)
	}
	if first != second {
		t.Fatalf("the rules depend on the order of the contract.\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
}

// TestEveryRedisServiceHasALogin: every service the contract names (a
// consumer, a producer or a redis_access entry) has a login, the migration
// login of streams-init is there too, and nothing else is. egress-proxy, which
// the contract never names, has none.
func TestEveryRedisServiceHasALogin(t *testing.T) {
	c := loadRealContract(t)
	logins, err := redisLogins(c)
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	have := map[string]string{}
	for _, l := range logins {
		have[l.Service] = l.Username
	}
	services := contractServices(c)
	for _, svc := range services {
		want := "continuo_" + strings.ReplaceAll(svc, "-", "_")
		if have[svc] != want {
			t.Errorf("service %s: login %q, want %q", svc, have[svc], want)
		}
	}
	for _, svc := range []string{"run-controller", "topology-controller", "ui", "agent-chat"} {
		if !slices.Contains(services, svc) {
			t.Errorf("the contract does not name %s", svc)
		}
	}
	if login, ok := have["egress-proxy"]; ok {
		t.Errorf("egress-proxy never opens a Redis connection, but has the login %q", login)
	}
	if have["streams-init"] != "continuo_redis_migrate" {
		t.Errorf("streams-init login: got %q, want continuo_redis_migrate", have["streams-init"])
	}
	if len(logins) != len(services)+1 {
		t.Errorf("got %d logins, want %d", len(logins), len(services)+1)
	}
	if !slices.IsSortedFunc(logins, func(a, b redisLogin) int { return strings.Compare(a.Service, b.Service) }) {
		t.Errorf("logins are not sorted by service: %+v", logins)
	}
}

// TestANewServiceGetsALogin: a service the contract names for the first time,
// as a producer, a consumer or a redis_access entry, gets its login from the
// contract alone.
func TestANewServiceGetsALogin(t *testing.T) {
	y := strings.Replace(aclFixtureYAML, "producers: [run-controller]\n", "producers: [run-controller, billing-controller]\n", 1)
	y = strings.Replace(y, "      - service: execution-controller\n        group: execution-controller-task-dispatch\n        const: ExecutionControllerTaskDispatch\n",
		"      - service: execution-controller\n        group: execution-controller-task-dispatch\n        const: ExecutionControllerTaskDispatch\n"+
			"      - service: billing-reader\n        group: billing-reader-task-dispatch\n        const: BillingReaderTaskDispatch\n", 1)
	y += "  - service: billing-ui\n    keys: [billing:]\n    commands: [get]\n"
	c := mustLoad(t, y)
	logins, err := redisLogins(c)
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	have := map[string]string{}
	for _, l := range logins {
		have[l.Service] = l.Rule
	}
	want := map[string]string{
		"billing-controller": "%W~continuo:task.dispatch:v1",
		"billing-reader":     "~continuo:task.dispatch:v1",
		"billing-ui":         "~continuo:billing:*",
	}
	for svc, pattern := range want {
		if !strings.HasPrefix(have[svc], pattern+" ") {
			t.Errorf("%s: rule %q, want it to start with %q", svc, have[svc], pattern)
		}
	}
}

// TestContractServicesAreRepositoryDirectories: the generator refuses a
// contract that names a service with no top-level directory of its own, so a
// misspelt service name gets no login.
func TestContractServicesAreRepositoryDirectories(t *testing.T) {
	c := mustLoad(t, aclFixtureYAML)
	root := t.TempDir()
	for _, svc := range []string{"run-controller", "execution-controller", "dead-letter-controller", "ui"} {
		if err := os.Mkdir(filepath.Join(root, svc), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := checkServiceDirectories(c, root); err != nil {
		t.Fatalf("every service has its directory: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "ui")); err != nil {
		t.Fatal(err)
	}
	err := checkServiceDirectories(c, root)
	if err == nil || !strings.Contains(err.Error(), `"ui"`) {
		t.Fatalf("expected an error naming ui, got %v", err)
	}
}

// TestRealContractServicesAreRepositoryDirectories runs the same check on the
// real contract and this repository.
func TestRealContractServicesAreRepositoryDirectories(t *testing.T) {
	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("findRepoRoot: %v", err)
	}
	if err := checkServiceDirectories(loadRealContract(t), root); err != nil {
		t.Fatal(err)
	}
}

// TestNoRuleGrantsDangerousCommands: no login gets a command group, or the
// commands that read or erase the whole keyspace.
func TestNoRuleGrantsDangerousCommands(t *testing.T) {
	c := loadRealContract(t)
	logins, err := redisLogins(c)
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	for _, l := range logins {
		for _, tok := range strings.Fields(l.Rule) {
			lower := strings.ToLower(tok)
			switch {
			case strings.Contains(lower, "@dangerous"), strings.Contains(lower, "@admin"):
				t.Errorf("%s: rule grants %s", l.Service, tok)
			case strings.HasPrefix(lower, "+@"):
				t.Errorf("%s: rule grants the command group %s", l.Service, tok)
			case lower == "+keys", strings.HasPrefix(lower, "+flush"), strings.HasPrefix(lower, "+keys|"):
				t.Errorf("%s: rule grants %s", l.Service, tok)
			case strings.HasPrefix(lower, "allcommands"), lower == "+@all", lower == "~*", lower == "allkeys":
				t.Errorf("%s: rule grants everything with %s", l.Service, tok)
			}
		}
		if !strings.Contains(l.Rule, " -@all ") {
			t.Errorf("%s: rule does not start from -@all: %s", l.Service, l.Rule)
		}
	}
}

// TestRedisAccessRejectsDangerousCommands: the contract cannot declare a
// command the generator must never grant.
func TestRedisAccessRejectsDangerousCommands(t *testing.T) {
	for _, cmd := range []string{"keys", "flushall", "flushdb", "@all", "@dangerous", "config|set", "acl", "KEYS", "Get"} {
		y := strings.Replace(aclFixtureYAML, "[get, set, del, expire]", `["`+cmd+`"]`, 1)
		_, err := loadAndValidate(strings.NewReader(y))
		if err == nil || !strings.Contains(err.Error(), "redis_access") {
			t.Errorf("command %q: expected a redis_access error, got %v", cmd, err)
		}
	}
}

func TestRedisAccessValidation(t *testing.T) {
	cases := map[string]string{
		"malformed service name":        strings.Replace(aclFixtureYAML, "service: ui", "service: UI_service", 1),
		"malformed producer name":       strings.Replace(aclFixtureYAML, "producers: [run-controller]", "producers: [run_controller]", 1),
		"key prefix without a colon":    strings.Replace(aclFixtureYAML, "[uisession:]", "[uisession]", 1),
		"key prefix that is a wildcard": strings.Replace(aclFixtureYAML, "[uisession:]", `["*"]`, 1),
		"duplicate service":             aclFixtureYAML + "  - service: ui\n    keys: [other:]\n    commands: [get]\n",
		"entry that grants nothing":     strings.Replace(aclFixtureYAML, "    keys: [uisession:]\n    commands: [get, set, del, expire]\n", "", 1),
	}
	for name, y := range cases {
		if _, err := loadAndValidate(strings.NewReader(y)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// TestRedisLoginsNeedTheDeadLetterStreams: a contract without the two
// dead-letter streams cannot produce rules, because no service could dead-letter.
func TestRedisLoginsNeedTheDeadLetterStreams(t *testing.T) {
	c := mustLoad(t, validYAML)
	if _, err := redisLogins(c); err == nil || !strings.Contains(err.Error(), "dead_letter") {
		t.Fatalf("expected a missing dead-letter stream error, got %v", err)
	}
}

// specRunControllerRule is the run-controller rule of section 7 of the
// per-service credentials design.
const specRunControllerRule = "~continuo:release.promoted:v2 ~continuo:task.status.updated:v2 " +
	"%W~continuo:task.dispatch:v1 %W~continuo:run.cancelled:v1 " +
	"%W~continuo:consumer.dead_letter:v1 %W~continuo:outbox.dead_letter:v1 " +
	"-@all +ping +hello +client|setinfo +xadd +xreadgroup +xack +xautoclaim +xclaim +xpending " +
	"+xinfo|consumers +xgroup|delconsumer"

// TestRunControllerRule: run-controller's generated rule grants exactly what
// the design's example grants, plus +xinfo|groups, which a consumer runs while
// it waits for its group.
func TestRunControllerRule(t *testing.T) {
	want := strings.Fields(specRunControllerRule)
	want = append(want, "+xinfo|groups")
	slices.Sort(want)

	c := loadRealContract(t)
	logins, err := redisLogins(c)
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	got := ruleOf(t, logins, "run-controller")
	gotTokens := strings.Fields(got)
	slices.Sort(gotTokens)
	if !slices.Equal(gotTokens, want) {
		t.Fatalf("run-controller rule differs from the design example plus +xinfo|groups.\n got: %s\nwant: %s", got, specRunControllerRule)
	}
	const exact = "~continuo:release.promoted:v2 ~continuo:task.status.updated:v2 " +
		"%W~continuo:consumer.dead_letter:v1 %W~continuo:outbox.dead_letter:v1 " +
		"%W~continuo:run.cancelled:v1 %W~continuo:task.dispatch:v1 " +
		"-@all +ping +hello +client|setinfo +xinfo|groups +xadd +xreadgroup +xack +xautoclaim +xclaim +xpending " +
		"+xinfo|consumers +xgroup|delconsumer"
	if got != exact {
		t.Errorf("run-controller rule:\n got: %s\nwant: %s", got, exact)
	}
}

// TestMigrationLoginRule pins the streams-init login: it creates streams and
// groups under the prefix and reaches nothing else.
func TestMigrationLoginRule(t *testing.T) {
	logins, err := redisLogins(loadRealContract(t))
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	const want = "~continuo:* -@all +ping +hello +client|setinfo +xgroup|create +xgroup|destroy +xinfo|groups +exists"
	if got := ruleOf(t, logins, "streams-init"); got != want {
		t.Errorf("streams-init rule:\n got: %s\nwant: %s", got, want)
	}
}

// TestNoLoginReachesABareStreamName: every key lives under the prefix, so no
// login's rule names a live or retired stream by its bare contract name.
func TestNoLoginReachesABareStreamName(t *testing.T) {
	c := loadRealContract(t)
	logins, err := redisLogins(c)
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	names := make([]string, 0, len(c.Streams)+len(c.RetiredStreams))
	for _, s := range c.Streams {
		names = append(names, s.Name)
	}
	for _, r := range c.RetiredStreams {
		names = append(names, r.Name)
	}
	for _, l := range logins {
		fields := strings.Fields(l.Rule)
		for _, name := range names {
			if slices.Contains(fields, "~"+name) || slices.Contains(fields, "%W~"+name) {
				t.Errorf("the rule of %s reaches the bare name %s: %s", l.Service, name, l.Rule)
			}
		}
	}
}

// TestEveryConsumerMayWriteItsDeadLetters: the services that write the
// consumer dead-letter stream (its producers list) include every service that
// consumes a stream.
func TestEveryConsumerMayWriteItsDeadLetters(t *testing.T) {
	c := loadRealContract(t)
	var consumerDL []string
	for _, s := range c.Streams {
		if s.Name == consumerDeadLetterStream {
			consumerDL = s.Producers
		}
	}
	for _, s := range c.Streams {
		for _, cons := range s.Consumers {
			if !slices.Contains(consumerDL, cons.Service) {
				t.Errorf("%s consumes %s but does not produce %s", cons.Service, s.Name, consumerDeadLetterStream)
			}
		}
	}
}

// TestDeadLetterControllerRule: dead-letter-controller reads and writes every
// prefixed stream.
func TestDeadLetterControllerRule(t *testing.T) {
	logins, err := redisLogins(loadRealContract(t))
	if err != nil {
		t.Fatalf("redisLogins: %v", err)
	}
	rule := ruleOf(t, logins, "dead-letter-controller")
	fields := strings.Fields(rule)
	for _, want := range []string{"~continuo:*", "+xadd", "+xtrim", "+xlen", "+del", "+info", "+xrange", "+xpending", "+xreadgroup"} {
		if !slices.Contains(fields, want) {
			t.Errorf("dead-letter-controller rule lacks %s: %s", want, rule)
		}
	}
	if slices.Contains(fields, "~*") {
		t.Errorf("dead-letter-controller rule reaches every key: %s", rule)
	}
}

func ruleOf(t *testing.T, logins []redisLogin, service string) string {
	t.Helper()
	for _, l := range logins {
		if l.Service == service {
			return l.Rule
		}
	}
	t.Fatalf("no login for %s", service)
	return ""
}
