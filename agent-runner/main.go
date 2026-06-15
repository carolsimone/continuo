package main

import (
	"log/slog"
	"os"
	"strings"

	"github.com/carolsimone/continuo/agent-runner/config"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	v := &pkgconfig.Validator{}
	cfg := config.Load(v)
	if missing := v.Missing(); len(missing) > 0 {
		logger.Error("startup configuration errors", "vars", strings.Join(missing, ", "))
		os.Exit(1)
	}
	_ = cfg
	logger.Info("agent-runner scaffold — wiring lands in a later task")
}
