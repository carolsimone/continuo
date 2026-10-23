// egress-proxy is the forward proxy agent-chat and agent-remediation reach the
// outside world through. It admits a connection only to a host and port on its
// allowlist, and only to addresses that pass its address check.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/carolsimone/continuo/egress-proxy/adapters/allowlistfile"
	"github.com/carolsimone/continuo/egress-proxy/adapters/http"
	"github.com/carolsimone/continuo/egress-proxy/adapters/httpproxy"
	"github.com/carolsimone/continuo/egress-proxy/adapters/metrics"
	"github.com/carolsimone/continuo/egress-proxy/config"
	"github.com/carolsimone/continuo/egress-proxy/service/authorize"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/lifecycle"
	"github.com/carolsimone/continuo/pkg/liveness"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

// dialTimeout bounds opening one connection to a destination.
const dialTimeout = 10 * time.Second

// headerTimeout bounds reading a client's request line and headers.
const headerTimeout = 10 * time.Second

// requestReasons is every reason the proxy records on a request: the listener's
// and the authorizer's. The metrics adapter exports each at 0 from startup.
var requestReasons = []string{
	httpproxy.ReasonOK,
	httpproxy.ReasonBadRequest,
	httpproxy.ReasonUnsupported,
	httpproxy.ReasonCapacity,
	httpproxy.ReasonDialFailed,
	httpproxy.ReasonUpstreamError,
	string(authorize.ReasonNotAllowed),
	string(authorize.ReasonPrivateAddress),
	string(authorize.ReasonMetadata),
	string(authorize.ReasonResolveFailed),
}

func main() {
	// The logger writes at the level LOG_LEVEL names. An unknown level is
	// recorded on v, and the missing-configuration check below stops the
	// service once this logger can report it.
	v := &pkgconfig.Validator{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: pkgconfig.LoadLogLevel(v)}))

	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("missing or invalid env vars", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	allow, err := allowlistfile.Load(cfg.AllowlistPath)
	if err != nil {
		logger.Error("invalid allowlist", "path", cfg.AllowlistPath, "error", err)
		os.Exit(1)
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	lifecycleManager := lifecycle.NewApplicationLifecycle(logger)
	lifecycleManager.SetupSignalHandlers(cancel, cfg.ShutdownGrace)

	liveReg := liveness.NewRegistry()
	metricsReg := pkgmetrics.New(config.Name)
	observer := metrics.New(metricsReg, requestReasons)
	startup := liveReg.AddStartupGate()

	healthServer := http.NewServer(cfg.HTTPPort, liveReg, logger)
	if err := healthServer.Listen(); err != nil {
		logger.Error("Failed to bind the health port", "port", cfg.HTTPPort, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Error("Health server error", "error", err)
		}
	}()

	metricsServer, err := pkgmetrics.Listen(cfg.MetricsPort, metricsReg, logger)
	if err != nil {
		logger.Error("Failed to bind the metrics port", "port", cfg.MetricsPort, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := metricsServer.Serve(); err != nil {
			logger.Error("Metrics server error", "error", err)
		}
	}()

	// Shutdown handlers run in reverse registration order: the proxy, registered
	// last, stops first, so health and metrics stay up while its tunnels drain.
	lifecycleManager.RegisterShutdownHandler(healthServer.Shutdown)
	lifecycleManager.RegisterShutdownHandler(metricsServer.Shutdown)

	// cfg.IdleTimeout is validated positive: a zero idle timeout would let a
	// silent tunnel or forwarded request hold its slot forever.
	auth := authorize.New(allow, net.DefaultResolver, &net.Dialer{Timeout: dialTimeout})
	proxy := httpproxy.New(auth, observer, logger, httpproxy.Options{
		MaxTunnels:    cfg.MaxTunnels,
		IdleTimeout:   cfg.IdleTimeout,
		HeaderTimeout: headerTimeout,
	})
	proxyAddr, err := proxy.Listen(cfg.ListenPort)
	if err != nil {
		logger.Error("Failed to bind the proxy port", "port", cfg.ListenPort, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := proxy.Serve(); err != nil {
			logger.Error("Proxy server error", "error", err)
		}
	}()
	lifecycleManager.RegisterShutdownHandler(proxy.Shutdown)

	// Listening, with the allowlist loaded: readiness may now turn true.
	startup.Done()

	logger.Info("egress-proxy started",
		"proxy_addr", proxyAddr.String(),
		"http_port", cfg.HTTPPort,
		"allowlist_entries", allow.Len(),
	)

	<-lifecycleManager.Done()
	logger.Info("Service stopped")
}
