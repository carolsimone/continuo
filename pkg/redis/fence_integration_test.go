//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The fence tests create ACL users with key patterns as wide as `~*`, so they
// run against a Redis of their own rather than the docker-compose one. The
// container starts on the first call of fenceRedis and is terminated by
// TestMain once the package's tests end: Ryuk is disabled in this repository,
// so nothing else reaps it.

const (
	fenceRedisImage = "redis:7.2-alpine"
	fencePassword   = "fence-test-password"
)

// fenceConnectionCommands are the commands every generated rule allows so a
// client can sign in and check its connection.
const fenceConnectionCommands = "+ping +hello +client|setinfo"

// fenceUsers are the logins the tests sign in as, by name, with the rule each
// one carries after `on >password`.
var fenceUsers = map[string]string{
	// fenced has the ui's shape: one key pattern, XINFO GROUPS allowed.
	"fenced": "~continuo:uisession:* -@all +xinfo|groups " + fenceConnectionCommands,
	// prefixwide has dead-letter-controller's and streams-init's shape: every
	// key under the continuo: prefix.
	"prefixwide": "~continuo:* -@all +xinfo|groups " + fenceConnectionCommands,
	// keywide may read every key with the commands of a fenced login.
	"keywide": "~* -@all +xinfo|groups " + fenceConnectionCommands,
	// open may run every command on every key.
	"open": "~* +@all",
}

var fenceRedisContainer struct {
	once      sync.Once
	container testcontainers.Container
	addr      string
	err       error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if c := fenceRedisContainer.container; c != nil {
		if err := c.Terminate(context.Background()); err != nil {
			os.Stderr.WriteString("terminate the fence test Redis container: " + err.Error() + "\n")
		}
	}
	os.Exit(code)
}

func startFenceRedis() {
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        fenceRedisImage,
			ExposedPorts: []string{"6379/tcp"},
			// The log line says Redis listens inside the container; the
			// listening-port wait also dials the mapped host port, which a VM's
			// port forward (colima, Docker Desktop) can open later.
			WaitingFor: wait.ForAll(
				wait.ForLog("Ready to accept connections"),
				wait.ForListeningPort("6379/tcp"),
			).WithDeadline(time.Minute),
		},
		Started: true,
	})
	if c != nil {
		fenceRedisContainer.container = c
	}
	if err != nil {
		fenceRedisContainer.err = err
		return
	}
	host, err := c.Host(ctx)
	if err != nil {
		fenceRedisContainer.err = err
		return
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		fenceRedisContainer.err = err
		return
	}
	addr := host + ":" + port.Port()
	admin := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { _ = admin.Close() }()
	for name, rule := range fenceUsers {
		args := []any{"ACL", "SETUSER", name, "reset", "on", ">" + fencePassword}
		for _, part := range strings.Fields(rule) {
			args = append(args, part)
		}
		if err := admin.Do(ctx, args...).Err(); err != nil {
			fenceRedisContainer.err = err
			return
		}
	}
	fenceRedisContainer.addr = addr
}

// fenceConfig returns the config of a client of the fence test Redis that
// signs in as user, in mode.
func fenceConfig(t *testing.T, user, mode string) pkgconfig.RedisConfig {
	t.Helper()
	fenceRedisContainer.once.Do(startFenceRedis)
	require.NoError(t, fenceRedisContainer.err, "start the fence test Redis")
	cfg, _, _ := redisConfigFromFile(t, fenceRedisContainer.addr, user, fencePassword)
	cfg.ACLMode = mode
	return cfg
}

// fenceClient returns a client of the fence test Redis signed in as user.
func fenceClient(t *testing.T, user string) *goredis.Client {
	t.Helper()
	client := pkgredis.NewClient(fenceConfig(t, user, pkgconfig.RedisACLModeShared))
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err(), "sign in as %s", user)
	return client
}

func TestFencePassesForAFencedLogin(t *testing.T) {
	client := fenceClient(t, "fenced")
	require.NoError(t, pkgredis.CheckPermissionFence(context.Background(), client, pkgconfig.RedisACLModePerService))
}

// A login limited to the continuo: prefix, as dead-letter-controller's and
// streams-init's are, passes: the probe key lies outside that prefix.
func TestFencePassesForAPrefixWideLogin(t *testing.T) {
	client := fenceClient(t, "prefixwide")
	require.NoError(t, pkgredis.CheckPermissionFence(context.Background(), client, pkgconfig.RedisACLModePerService))
}

func TestFenceFailsForAKeyWideLogin(t *testing.T) {
	client := fenceClient(t, "keywide")
	err := pkgredis.CheckPermissionFence(context.Background(), client, pkgconfig.RedisACLModePerService)
	require.Error(t, err)
	assert.Equal(t,
		"redis login keywide can read keys outside its own: apply the rules in deploy/redis-acl-rules.acl",
		err.Error())
}

func TestFenceFailsForAnOpenLogin(t *testing.T) {
	client := fenceClient(t, "open")
	err := pkgredis.CheckPermissionFence(context.Background(), client, pkgconfig.RedisACLModePerService)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis login open can read keys outside its own")
}

func TestFenceSkipsSharedMode(t *testing.T) {
	client := fenceClient(t, "open")
	require.NoError(t, pkgredis.CheckPermissionFence(context.Background(), client, pkgconfig.RedisACLModeShared))
}

// generatedRulesFile is the machine-readable copy of the generated Redis rules,
// one entry per login.
var generatedRulesFile = filepath.Join("..", "..", "deploy", "continuo", "files", "redis-acl-rules.json")

// Every login the rules generator emits passes the fence: the probe key lies
// outside each of their key patterns.
func TestFencePassesForEveryGeneratedLogin(t *testing.T) {
	raw, err := os.ReadFile(generatedRulesFile)
	require.NoError(t, err)
	var rules struct {
		Logins []struct {
			Username string `json:"username"`
			Rule     string `json:"rule"`
		} `json:"logins"`
	}
	require.NoError(t, json.Unmarshal(raw, &rules))
	require.NotEmpty(t, rules.Logins)

	fenceRedisContainer.once.Do(startFenceRedis)
	require.NoError(t, fenceRedisContainer.err, "start the fence test Redis")
	ctx := context.Background()
	admin := goredis.NewClient(&goredis.Options{Addr: fenceRedisContainer.addr})
	t.Cleanup(func() { _ = admin.Close() })
	for _, login := range rules.Logins {
		t.Run(login.Username, func(t *testing.T) {
			args := []any{"ACL", "SETUSER", login.Username, "reset", "on", ">" + fencePassword}
			for _, part := range strings.Fields(login.Rule) {
				args = append(args, part)
			}
			require.NoError(t, admin.Do(ctx, args...).Err())
			t.Cleanup(func() { _ = admin.Do(context.Background(), "ACL", "DELUSER", login.Username).Err() })

			client := fenceClient(t, login.Username)
			require.NoError(t, pkgredis.CheckPermissionFence(ctx, client, pkgconfig.RedisACLModePerService))
		})
	}
}

// Connect runs the fence once Redis answers PING, so a key-wide login in
// perService mode never reaches the service.
func TestConnectRefusesAKeyWideLoginInPerServiceMode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := fenceConfig(t, "keywide", pkgconfig.RedisACLModePerService)
	client, err := pkgredis.Connect(context.Background(), cfg, pkgredis.DefaultStartupPolicy(), logger)
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "redis login keywide can read keys outside its own")

	cfg = fenceConfig(t, "fenced", pkgconfig.RedisACLModePerService)
	client, err = pkgredis.Connect(context.Background(), cfg, pkgredis.DefaultStartupPolicy(), logger)
	require.NoError(t, err)
	_ = client.Close()
}
