// Package schedulesconfig adapts the schedules file the install mounts to the
// ports that read the cron configuration of its schedules.
package schedulesconfig

import (
	"github.com/carolsimone/continuo/state/internal/scheduler"
	"github.com/carolsimone/continuo/state/service/ports"
)

var _ ports.ScheduleCrons = (*Crons)(nil)

// Crons gives the cron entries of a loaded schedules file.
type Crons struct {
	timezone string
	byName   map[string]ports.ScheduleCron
}

// New returns the Crons of cfg; a nil cfg declares no schedule.
func New(cfg *scheduler.SchedulesConfig) *Crons {
	c := &Crons{byName: map[string]ports.ScheduleCron{}}
	if cfg == nil {
		return c
	}
	c.timezone = cfg.Timezone
	for _, e := range cfg.Schedules {
		c.byName[e.Name] = ports.ScheduleCron{Expression: e.Cron, Description: e.Description}
	}
	return c
}

// Timezone is the IANA timezone the file's cron expressions are read in.
func (c *Crons) Timezone() string { return c.timezone }

// CronOf returns the cron entry of the named schedule.
func (c *Crons) CronOf(scheduleName string) (ports.ScheduleCron, bool) {
	e, ok := c.byName[scheduleName]
	return e, ok
}
