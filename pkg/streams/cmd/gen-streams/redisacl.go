package main

// Redis ACL rules. Each service that opens a Redis connection signs in as its
// own login, and the rule of that login is derived here from the streams the
// contract says the service produces and consumes, plus the plain-key grants of
// the contract's redis_access list.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// RedisAccess is what one service may do in Redis beyond reading the streams
// it consumes and writing the streams it produces.
type RedisAccess struct {
	Service string `yaml:"service"`
	// Keys lists plain-key prefixes (after the contract's key prefix) the
	// service reads and writes with the commands below.
	Keys []string `yaml:"keys"`
	// Commands are the commands the service may run beyond its stream commands.
	Commands []string `yaml:"commands"`
	// AllStreams reaches every key under the contract's key prefix.
	AllStreams bool `yaml:"all_streams"`
}

const (
	// consumerDeadLetterStream and outboxDeadLetterStream are the two streams
	// every consumer and every outbox writes its dead letters to; their
	// producers lists name who may write them.
	consumerDeadLetterStream = "consumer.dead_letter:v1"
	outboxDeadLetterStream   = "outbox.dead_letter:v1"

	// streamsInitService is the deploy step that creates every stream and
	// consumer group. Its login is the Redis migration login.
	streamsInitService  = "streams-init"
	streamsInitUsername = "continuo_redis_migrate"

	// redisBaseCommands are what every login may run, after -@all: the
	// connection handshake and the group lookup a consumer waits on.
	redisBaseCommands = "-@all +ping +hello +client|setinfo +xinfo|groups"
	// redisProducerCommands is what a login that writes a stream may run.
	redisProducerCommands = "+xadd"
	// redisConsumerCommands is what a login that consumes a stream may run.
	redisConsumerCommands = "+xreadgroup +xack +xautoclaim +xclaim +xpending +xinfo|consumers +xgroup|delconsumer"
	// redisMigrationCommands are the commands of the migration login, after
	// the key pattern ~<prefix>*.
	redisMigrationCommands = "-@all +ping +hello +client|setinfo +xgroup|create +xgroup|destroy +xinfo|groups +exists +type"
)

// serviceNameRe is the shape of a service name: lowercase words joined by
// hyphens, the name of the service's top-level directory.
var serviceNameRe = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// contractServices lists, sorted, every service the contract names: the
// producers and consumers of its streams and the services of its redis_access
// list. Each of them signs in to Redis as its own login; a service the
// contract never names (egress-proxy) opens no Redis connection.
func contractServices(c *Contract) []string {
	services := map[string]struct{}{}
	for _, s := range c.Streams {
		for _, p := range s.Producers {
			services[p] = struct{}{}
		}
		for _, cons := range s.Consumers {
			services[cons.Service] = struct{}{}
		}
	}
	for _, a := range c.RedisAccess {
		services[a.Service] = struct{}{}
	}
	return sortedKeys(services)
}

// validateServiceNames checks that every service the contract names has the
// shape of a service name, so each login name derived from it is well formed.
func validateServiceNames(c *Contract) error {
	for _, svc := range contractServices(c) {
		if !serviceNameRe.MatchString(svc) {
			return fmt.Errorf("service %q must match %s", svc, serviceNameRe.String())
		}
	}
	return nil
}

// checkServiceDirectories checks that every service the contract names is a
// top-level directory under root, so a misspelt service name fails the
// generator instead of getting a login no process signs in as.
func checkServiceDirectories(c *Contract, root string) error {
	var missing []string
	for _, svc := range contractServices(c) {
		info, err := os.Stat(filepath.Join(root, svc))
		if err != nil || !info.IsDir() {
			missing = append(missing, fmt.Sprintf("%q", svc))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the contract names services with no directory under %s: %s", root, strings.Join(missing, ", "))
	}
	return nil
}

var (
	redisKeyPrefixRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(:[a-z0-9._-]+)*:$`)
	redisCommandRe    = regexp.MustCompile(`^[a-z][a-z0-9_]*(\|[a-z][a-z0-9_]*)?$`)
	redisForbiddenCmd = []string{"keys", "flushall", "flushdb", "config", "acl", "debug", "shutdown", "monitor", "save", "bgsave", "bgrewriteaof", "replicaof", "slaveof", "migrate", "module", "script"}
)

// validateRedisAccess checks the redis_access list: each entry names a
// service once, grants something, and names only plain commands that are not
// administrative or whole-keyspace commands.
func validateRedisAccess(c *Contract) error {
	seen := map[string]struct{}{}
	for i, a := range c.RedisAccess {
		if _, dup := seen[a.Service]; dup {
			return fmt.Errorf("redis_access[%d]: duplicate service %q", i, a.Service)
		}
		seen[a.Service] = struct{}{}
		if len(a.Keys) == 0 && !a.AllStreams {
			return fmt.Errorf("redis_access[%d] (%s): names no keys and not all_streams, so it grants nothing", i, a.Service)
		}
		if len(a.Commands) == 0 {
			return fmt.Errorf("redis_access[%d] (%s): names no commands", i, a.Service)
		}
		for _, k := range a.Keys {
			if !redisKeyPrefixRe.MatchString(k) {
				return fmt.Errorf("redis_access[%d] (%s): key prefix %q must match %s", i, a.Service, k, redisKeyPrefixRe.String())
			}
		}
		for _, cmd := range a.Commands {
			if !redisCommandRe.MatchString(cmd) {
				return fmt.Errorf("redis_access[%d] (%s): command %q must match %s", i, a.Service, cmd, redisCommandRe.String())
			}
			if slices.Contains(redisForbiddenCmd, strings.SplitN(cmd, "|", 2)[0]) || strings.HasPrefix(cmd, "flush") {
				return fmt.Errorf("redis_access[%d] (%s): command %q is administrative or reaches the whole keyspace and is never granted", i, a.Service, cmd)
			}
		}
	}
	return nil
}

// redisLogin is one Redis user: the service that signs in as it, its name in
// Redis and its rule, which holds no password.
type redisLogin struct {
	Service  string `json:"service"`
	Username string `json:"username"`
	Rule     string `json:"rule"`
}

// redisUsername is the login name of a service.
func redisUsername(service string) string {
	return "continuo_" + strings.ReplaceAll(service, "-", "_")
}

// redisLogins returns the login of every service the contract names plus the
// migration login, sorted by service.
func redisLogins(c *Contract) ([]redisLogin, error) {
	var consumerDL, outboxDL *Stream
	for i := range c.Streams {
		switch c.Streams[i].Name {
		case consumerDeadLetterStream:
			consumerDL = &c.Streams[i]
		case outboxDeadLetterStream:
			outboxDL = &c.Streams[i]
		}
	}
	if consumerDL == nil || outboxDL == nil {
		return nil, fmt.Errorf("the contract must declare %s and %s: every service writes its dead letters there", consumerDeadLetterStream, outboxDeadLetterStream)
	}

	services := contractServices(c)
	logins := make([]redisLogin, 0, len(services)+1)
	for _, svc := range services {
		logins = append(logins, redisLogin{Service: svc, Username: redisUsername(svc), Rule: redisRule(c, svc)})
	}
	logins = append(logins, redisLogin{
		Service:  streamsInitService,
		Username: streamsInitUsername,
		Rule:     fmt.Sprintf("~%s* %s", c.KeyPrefix, redisMigrationCommands),
	})
	sort.Slice(logins, func(i, j int) bool { return logins[i].Service < logins[j].Service })
	return logins, nil
}

// redisRule builds the rule of one service: its key patterns (the prefixed
// streams, then the unprefixed ones while legacy_keys holds), then the
// commands those patterns need.
func redisRule(c *Contract, service string) string {
	read := map[string]struct{}{}  // streams the service consumes
	write := map[string]struct{}{} // streams the service produces and does not consume
	produced := false
	for _, st := range c.Streams {
		if slices.ContainsFunc(st.Consumers, func(cons Consumer) bool { return cons.Service == service }) {
			read[st.Name] = struct{}{}
		}
	}
	for _, st := range c.Streams {
		if !slices.Contains(st.Producers, service) {
			continue
		}
		produced = true
		if _, consumed := read[st.Name]; !consumed {
			write[st.Name] = struct{}{}
		}
	}
	consumes := len(read) > 0
	produces := produced

	var access *RedisAccess
	for i := range c.RedisAccess {
		if c.RedisAccess[i].Service == service {
			access = &c.RedisAccess[i]
		}
	}

	readNames := sortedKeys(read)
	writeNames := sortedKeys(write)

	var patterns []string
	prefixed := func(name string) string { return c.KeyPrefix + name }
	add := func(mode, key string) { patterns = append(patterns, mode+key) }
	allStreams := access != nil && access.AllStreams
	if !allStreams {
		for _, n := range readNames {
			add("~", prefixed(n))
		}
		for _, n := range writeNames {
			add("%W~", prefixed(n))
		}
	}
	if access != nil {
		for _, k := range access.Keys {
			add("~", c.KeyPrefix+k+"*")
		}
		if access.AllStreams {
			add("~", c.KeyPrefix+"*")
		}
	}
	if c.LegacyKeys {
		if allStreams {
			for _, n := range allStreamNames(c) {
				add("~", n)
			}
		} else {
			for _, n := range readNames {
				add("~", n)
			}
			for _, n := range writeNames {
				add("%W~", n)
			}
		}
	}

	commands := strings.Fields(redisBaseCommands)
	if produces {
		commands = append(commands, redisProducerCommands)
	}
	if consumes {
		commands = append(commands, strings.Fields(redisConsumerCommands)...)
	}
	if access != nil {
		for _, cmd := range access.Commands {
			commands = append(commands, "+"+cmd)
		}
	}
	return strings.Join(append(patterns, dedupe(commands)...), " ")
}

// allStreamNames lists every live and retired stream, sorted.
func allStreamNames(c *Contract) []string {
	names := make([]string, 0, len(c.Streams)+len(c.RetiredStreams))
	for _, s := range c.Streams {
		names = append(names, s.Name)
	}
	for _, r := range c.RetiredStreams {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return names
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// dedupe drops repeats, keeping the first occurrence of each token.
func dedupe(tokens []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// emitRedisACLJSON renders the machine-readable rules: the key prefix and one
// entry per login.
func emitRedisACLJSON(c *Contract) (string, error) {
	logins, err := redisLogins(c)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	prefix, err := marshalNoEscape(c.KeyPrefix)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "{\n  \"key_prefix\": %s,\n  \"logins\": [\n", prefix)
	for i, l := range logins {
		line, err := marshalNoEscape(l)
		if err != nil {
			return "", err
		}
		sep := ","
		if i == len(logins)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s%s\n", line, sep)
	}
	b.WriteString("  ]\n}\n")
	return b.String(), nil
}

func marshalNoEscape(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

// emitRedisACL renders the ACL file published with the chart: one passwordless
// user line per login.
func emitRedisACL(c *Contract) (string, error) {
	logins, err := redisLogins(c)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("# continuo Redis ACL rules: one login per service, generated from pkg/streams/contract.yaml by `go generate ./pkg/streams/...`. Do not edit.\n")
	b.WriteString("# Each line is a login without a password: add `>password` or `#<sha256>` after `on`. Redis refuses comment lines in an ACL file, so drop these two lines when you load it.\n")
	for _, l := range logins {
		fmt.Fprintf(&b, "user %s on %s\n", l.Username, l.Rule)
	}
	return b.String(), nil
}

// emitRedisComposeACL renders the ACL file docker compose starts Redis with:
// every login has the local password local-<username>, and the default user
// keeps the shared compose password so test suites and the e2e harness sign in
// as before. The file has no comment lines, which Redis refuses in an ACL file.
func emitRedisComposeACL(c *Contract) (string, error) {
	logins, err := redisLogins(c)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("user default on >continuo ~* &* +@all\n")
	for _, l := range logins {
		fmt.Fprintf(&b, "user %s on >local-%s %s\n", l.Username, l.Username, l.Rule)
	}
	return b.String(), nil
}

// writeRedisACLFiles writes the three ACL outputs under root and returns their
// paths.
func writeRedisACLFiles(c *Contract, root string) ([]string, error) {
	outputs := []struct {
		path string
		emit func(*Contract) (string, error)
	}{
		{filepath.Join(root, "deploy", "continuo", "files", "redis-acl-rules.json"), emitRedisACLJSON},
		{filepath.Join(root, "deploy", "redis-acl-rules.acl"), emitRedisACL},
		{filepath.Join(root, "compose", "redis-users.acl"), emitRedisComposeACL},
	}
	paths := make([]string, 0, len(outputs))
	for _, o := range outputs {
		src, err := o.emit(c)
		if err != nil {
			return nil, fmt.Errorf("emit %s: %w", filepath.Base(o.path), err)
		}
		if err := os.MkdirAll(filepath.Dir(o.path), 0o750); err != nil {
			return nil, fmt.Errorf("create %s: %w", filepath.Dir(o.path), err)
		}
		if err := os.WriteFile(o.path, []byte(src), 0o600); err != nil {
			return nil, fmt.Errorf("write %s: %w", o.path, err)
		}
		paths = append(paths, o.path)
	}
	return paths, nil
}
