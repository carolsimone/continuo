// Command streams-init creates every Redis stream and consumer group of the
// release under its prefixed key, then exits. It is the one step that creates
// them: consumers wait for their group and producers never create a stream.
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
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := goredis.NewClient(&goredis.Options{Addr: redisCfg.Addr(), Password: redisCfg.Password})
	defer func() { _ = client.Close() }()

	if createStreams {
		if err := pkgredis.WaitForRedis(ctx, client, redisWait, logger); err != nil {
			logger.Error("Failed to connect to Redis", "addr", redisCfg.Addr(), "error", err)
			return 1
		}
	}

	plan := streamsinit.Plan{Groups: streams.Groups, CreateStreams: createStreams}
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
