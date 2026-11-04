// Command streams-init signs in as every Redis login of REDIS_LOGINS_DIR, then
// creates every Redis stream and consumer group of the release under its
// prefixed key, and exits. It is the one step that creates them: consumers
// wait for their group and producers never create a stream. Its own login
// passes the permission fence of REDIS_ACL_MODE before it does either.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/adapters/loginfiles"
	redisadapter "github.com/carolsimone/continuo/dead-letter-controller/adapters/redis"
	"github.com/carolsimone/continuo/dead-letter-controller/service/streamsinit"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// redisWait bounds how long the command waits for Redis to answer, so a Redis
// that is still starting is waited for and an unreachable one fails the run.
var redisWait = pkgredis.StartupPolicy{Timeout: 30 * time.Second, InitialDelay: time.Second, MaxDelay: 5 * time.Second}

func main() { os.Exit(run()) }

func run() int {
	v := &pkgconfig.Validator{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: pkgconfig.LoadLogLevel(v)}))
	redisCfg := pkgconfig.LoadRedisFromAddr(v)
	createStreams := pkgconfig.EnvBoolOrDefault("REDIS_INIT_CREATE_STREAMS", true)
	loginsDir := pkgconfig.EnvOrDefault("REDIS_LOGINS_DIR", "")
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The command talks to Redis only to create streams or to check logins;
	// with neither to do it never connects.
	var client *goredis.Client
	if createStreams || loginsDir != "" {
		var err error
		if client, err = pkgredis.Connect(ctx, redisCfg, redisWait, logger); err != nil {
			logger.Error("Failed to connect to Redis", "addr", redisCfg.Addr(), "error", err)
			return 1
		}
	} else {
		client = pkgredis.NewClient(redisCfg)
	}
	defer func() { _ = client.Close() }()

	plan := streamsinit.Plan{Groups: streams.Groups, CreateStreams: createStreams}
	if loginsDir != "" {
		plan.Logins = loginfiles.New(loginsDir)
	}
	if err := streamsinit.Run(ctx, redisadapter.NewStreamAdmin(client), plan, logger); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Shutdown requested")
			return 1
		}
		logger.Error("streams-init failed", "addr", redisCfg.Addr(), "error", err)
		return 1
	}
	logger.Info("streams-init done")
	return 0
}
