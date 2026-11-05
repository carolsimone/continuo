package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	goredis "github.com/redis/go-redis/v9"
)

// permissionProbeKey is the key the permission fence asks Redis about. It lies
// outside every key pattern of the generated rules, all of them under the
// continuo: prefix, so a login built from those rules is always refused it.
const permissionProbeKey = "continuo-permission-probe"

// CheckPermissionFence makes sure, in RedisACLModePerService, that the login
// client signs in as cannot reach keys outside its own. It runs XINFO GROUPS
// on permissionProbeKey: every generated rule allows that command, so a NOPERM
// answer can only mean the key is outside the login's patterns, and any other
// answer (the key's groups, "no such key", another error reply) means the
// login reaches it. A probe Redis does not answer (a dropped connection, a
// cancelled ctx) is returned as a failed check, not as a fence violation. In
// RedisACLModeShared it checks nothing; any other mode is an error.
func CheckPermissionFence(ctx context.Context, client *goredis.Client, mode string) error {
	switch mode {
	case pkgconfig.RedisACLModeShared:
		return nil
	case pkgconfig.RedisACLModePerService:
	default:
		return fmt.Errorf("unknown REDIS_ACL_MODE %q: expected %s or %s",
			mode, pkgconfig.RedisACLModeShared, pkgconfig.RedisACLModePerService)
	}
	err := client.Do(ctx, "XINFO", "GROUPS", permissionProbeKey).Err()
	return fenceVerdict(loginName(ctx, client), err)
}

// fenceVerdict turns the probe's answer into the fence's result for user.
func fenceVerdict(user string, probeErr error) error {
	var reply goredis.Error
	switch {
	case probeErr != nil && errors.As(probeErr, &reply) && strings.HasPrefix(reply.Error(), "NOPERM"):
		return nil
	case probeErr != nil && isStartupRetryable(probeErr):
		return fmt.Errorf("check the permissions of redis login %s: %w", user, probeErr)
	default:
		return fmt.Errorf("redis login %s can read keys outside its own: apply the rules in deploy/redis-acl-rules.acl", user)
	}
}

// loginName is the Redis user client signs in as: its username, from the
// options or from the credentials provider NewClient installs, and "default"
// when it names none.
func loginName(ctx context.Context, client *goredis.Client) string {
	opts := client.Options()
	user := opts.Username
	if user == "" && opts.CredentialsProviderContext != nil {
		if name, _, err := opts.CredentialsProviderContext(ctx); err == nil {
			user = name
		}
	}
	if user == "" {
		return "default"
	}
	return user
}
