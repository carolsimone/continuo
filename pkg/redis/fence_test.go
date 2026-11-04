package redis

import (
	"context"
	"errors"
	"io"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const keywideViolation = "redis login keywide can read keys outside its own: apply the rules in deploy/redis-acl-rules.acl"

func TestFenceVerdictPassesOnlyOnNoPerm(t *testing.T) {
	require.NoError(t, fenceVerdict("keywide",
		replyErr("NOPERM User keywide has no permissions to access the 'continuo-permission-probe' key")))
}

// Any answer Redis gives other than NOPERM means the login reached the probe
// key: a missing key, a key it can read, or another error reply.
func TestFenceVerdictFailsOnAnyOtherAnswer(t *testing.T) {
	for name, answer := range map[string]error{
		"success":     nil,
		"no such key": replyErr("ERR no such key"),
		"wrong type":  replyErr("WRONGTYPE Operation against a key holding the wrong kind of value"),
	} {
		t.Run(name, func(t *testing.T) {
			err := fenceVerdict("keywide", answer)
			require.Error(t, err)
			assert.Equal(t, keywideViolation, err.Error())
		})
	}
}

// A failure that is not Redis's answer (a dropped connection, a Redis that is
// loading) says nothing about the login's keys: it is returned as a failed
// check, not as a fence violation.
func TestFenceVerdictDoesNotReportAnUnansweredProbeAsAViolation(t *testing.T) {
	for name, answer := range map[string]error{
		"connection dropped": io.EOF,
		"loading":            replyErr("LOADING Redis is loading the dataset in memory"),
		"cancelled":          context.Canceled,
	} {
		t.Run(name, func(t *testing.T) {
			err := fenceVerdict("keywide", answer)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "can read keys outside its own")
			assert.Contains(t, err.Error(), "check the permissions of redis login keywide")
			assert.True(t, errors.Is(err, answer), "the error wraps the probe's failure")
		})
	}
}

func TestCheckPermissionFenceRefusesAnUnknownMode(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "redis.invalid:6379"})
	defer func() { _ = client.Close() }()
	err := CheckPermissionFence(context.Background(), client, "perservice")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown REDIS_ACL_MODE "perservice"`)
}

func TestCheckPermissionFenceSkipsSharedModeWithoutAskingRedis(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "redis.invalid:6379"})
	defer func() { _ = client.Close() }()
	require.NoError(t, CheckPermissionFence(context.Background(), client, pkgconfig.RedisACLModeShared))
}

func TestLoginNameIsTheConfiguredUsernameOrDefault(t *testing.T) {
	cfg := pkgconfig.RedisConfig{Host: "redis.invalid", Port: 6379, Username: "continuo_ui"}
	named := NewClient(cfg)
	defer func() { _ = named.Close() }()
	assert.Equal(t, "continuo_ui", loginName(context.Background(), named))

	plain := goredis.NewClient(&goredis.Options{Addr: "redis.invalid:6379", Username: "keywide"})
	defer func() { _ = plain.Close() }()
	assert.Equal(t, "keywide", loginName(context.Background(), plain))

	unnamed := goredis.NewClient(&goredis.Options{Addr: "redis.invalid:6379"})
	defer func() { _ = unnamed.Close() }()
	assert.Equal(t, "default", loginName(context.Background(), unnamed))
}
