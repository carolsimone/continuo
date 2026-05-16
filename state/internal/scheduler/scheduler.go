package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// SchedulesConfig holds the full cron schedule configuration loaded from schedules.yaml.
type SchedulesConfig struct {
	Timezone    string          `yaml:"timezone"`
	Schedules   []ScheduleEntry `yaml:"schedules"`
	WithSeconds bool            // injected at runtime for tests; not in YAML
}

// ScheduleEntry is a single named cron schedule.
type ScheduleEntry struct {
	Name        string `yaml:"name"`
	Cron        string `yaml:"cron"`
	Description string `yaml:"description"`
}

// LoadSchedulesConfig reads and parses the YAML file at path.
// Returns an error if the file is missing or malformed.
func LoadSchedulesConfig(path string) (*SchedulesConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read schedules config %q: %w", path, err)
	}
	var cfg SchedulesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse schedules config %q: %w", path, err)
	}
	if cfg.Timezone == "" {
		return nil, fmt.Errorf("schedules config %q: timezone is required", path)
	}
	if len(cfg.Schedules) == 0 {
		return nil, fmt.Errorf("schedules config %q: at least one schedule is required", path)
	}
	return &cfg, nil
}

// CronScheduler manages scheduled activations driven by schedules.yaml.
type CronScheduler struct {
	cron       *cron.Cron
	activate   *svchandlers.ActivateScheduleHandler
	uowFactory func() uow.UnitOfWork
	config     *SchedulesConfig
	logger     *slog.Logger
}

// NewCronSchedulerWithConfig creates a CronScheduler from the provided config.
// Fails if the timezone is invalid or any cron expression is malformed.
func NewCronSchedulerWithConfig(activate *svchandlers.ActivateScheduleHandler, uowFactory func() uow.UnitOfWork, logger *slog.Logger, cfg *SchedulesConfig) (*CronScheduler, error) {
	location, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %q: %w", cfg.Timezone, err)
	}

	var cronScheduler *cron.Cron
	if cfg.WithSeconds {
		cronScheduler = cron.New(cron.WithLocation(location), cron.WithSeconds())
	} else {
		cronScheduler = cron.New(cron.WithLocation(location))
	}

	s := &CronScheduler{
		cron:       cronScheduler,
		activate:   activate,
		uowFactory: uowFactory,
		config:     cfg,
		logger:     logger,
	}

	for _, entry := range cfg.Schedules {
		name := entry.Name // capture for closure
		_, err := cronScheduler.AddFunc(entry.Cron, func() {
			s.activateSchedule(name)
		})
		if err != nil {
			return nil, fmt.Errorf("invalid cron expression for schedule %q: %w", name, err)
		}
		logger.Info("Registered cron schedule", "name", name, "cron", entry.Cron)
	}

	logger.Info("Cron scheduler initialized",
		"timezone", location.String(),
		"schedules", len(cfg.Schedules),
	)
	return s, nil
}

// Start starts the cron scheduler.
func (s *CronScheduler) Start() error {
	s.logger.Info("Starting cron scheduler")
	s.cron.Start()
	return nil
}

// Stop gracefully stops the cron scheduler.
func (s *CronScheduler) Stop(ctx context.Context) error {
	s.logger.Info("Stopping cron scheduler")
	stopCtx := s.cron.Stop()
	select {
	case <-stopCtx.Done():
		s.logger.Info("Cron scheduler stopped")
		return nil
	case <-ctx.Done():
		s.logger.Warn("Cron scheduler stop timed out")
		return ctx.Err()
	}
}

func (s *CronScheduler) activateSchedule(name string) {
	s.logger.Info("Cron trigger fired", "schedule_name", name)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, err := s.activate.Handle(ctx, s.uowFactory(), name, run.KindCron, nil)
	if err != nil {
		s.logger.Error("Failed to activate schedule", "schedule_name", name, "error", err)
	}
}
