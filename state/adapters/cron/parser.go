// Package cron reads the cron expressions of schedules: it parses a cron in a
// timezone into the expression that says when it fires, and describes a cron
// in English words. It implements ports.CronParser over robfig/cron's standard
// parser and lnquy/cron's descriptor, which only this package imports.
package cron

import (
	"errors"
	"fmt"
	"strings"
	"time"

	lnquy "github.com/lnquy/cron"
	robfig "github.com/robfig/cron/v3"

	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/service/ports"
)

var _ ports.CronParser = (*Parser)(nil)

// minInterval is the shortest `@every` interval: a schedule fires at minute
// granularity, as the five cron fields do.
const minInterval = time.Minute

// neverFiresProbe is the instant a parsed cron must fire after, to be taken as
// firing at all. The expressions that fail it name a date no calendar has.
var neverFiresProbe = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// Parser parses crons into schedule.Expression and describes them in words.
// It is safe for concurrent use.
type Parser struct {
	// words puts five cron fields in English.
	words func(fields string) (string, error)
}

// NewParser returns a Parser whose descriptions are English, with times on a
// 24-hour clock.
func NewParser() (*Parser, error) {
	descriptor, err := lnquy.NewDescriptor(lnquy.Use24HourTimeFormat(true))
	if err != nil {
		return nil, fmt.Errorf("cron descriptor: %w", err)
	}
	return &Parser{words: func(fields string) (string, error) {
		return descriptor.ToDescription(fields, lnquy.Locale_en)
	}}, nil
}

// Parse reads cron, five fields or a descriptor such as `@daily`, in the IANA
// timezone. The zone is a separate argument, so a `CRON_TZ=` or `TZ=` prefix is
// refused, as is a zone name that is empty, `Local` or unknown, and a cron that
// no date satisfies.
func (p *Parser) Parse(cron, timezone string) (schedule.Expression, error) {
	spec, err := parseSpec(cron)
	if err != nil {
		return nil, err
	}
	loc, err := loadZone(timezone)
	if err != nil {
		return nil, err
	}
	expr := newExpression(spec, loc)
	if expr.Next(neverFiresProbe).IsZero() {
		return nil, fmt.Errorf("cron %q never fires: no date satisfies it", strings.TrimSpace(cron))
	}
	return expr, nil
}

// Describe returns cron in English words. It refuses what Parse refuses, and a
// cron the words library cannot read, such as a day-of-week step of `*/9`.
func (p *Parser) Describe(cron string) (string, error) {
	spec, err := parseSpec(cron)
	if err != nil {
		return "", err
	}
	if every, ok := spec.(robfig.ConstantDelaySchedule); ok {
		return "Every " + every.Delay.String(), nil
	}
	text := strings.TrimSpace(cron)
	fields, ok := descriptorFields[text]
	if !ok {
		// robfig reads `?` as `*`; the words library cannot read it in every field.
		fields = strings.ReplaceAll(text, "?", "*")
	}
	words, err := p.speak(fields)
	if err != nil {
		return "", fmt.Errorf("cron %q cannot be put in words: %w", text, err)
	}
	return words, nil
}

// speak puts fields in words. The words library runs on text an operator typed,
// so a panic inside it becomes an error rather than the end of the process.
func (p *Parser) speak(fields string) (words string, err error) {
	defer func() {
		if r := recover(); r != nil {
			words, err = "", fmt.Errorf("the descriptor failed on it: %v", r)
		}
	}()
	return p.words(fields)
}

// descriptorFields gives the five fields each calendar descriptor stands for.
var descriptorFields = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// parseSpec reads cron with robfig's standard parser, after refusing a zone
// prefix it would otherwise honour. It refuses an `@every` interval shorter
// than minInterval; robfig rounds any interval to whole seconds, so `@every 1s`
// is the shortest it would otherwise fire.
func parseSpec(cron string) (robfig.Schedule, error) {
	text := strings.TrimSpace(cron)
	if text == "" {
		return nil, errors.New("cron is empty")
	}
	if strings.HasPrefix(text, "CRON_TZ=") || strings.HasPrefix(text, "TZ=") {
		return nil, errors.New("cron must not start with CRON_TZ= or TZ=: the timezone is a separate field")
	}
	spec, err := robfig.ParseStandard(text)
	if err != nil {
		return nil, fmt.Errorf("cron %q: %w", text, err)
	}
	if every, ok := spec.(robfig.ConstantDelaySchedule); ok && every.Delay < minInterval {
		return nil, fmt.Errorf("cron %q: an @every interval is at least one minute", text)
	}
	return spec, nil
}

// loadZone returns the IANA zone of the name. The empty name and `Local` are
// not IANA zone names, whatever the process's own zone is.
func loadZone(name string) (*time.Location, error) {
	if name != "" && name != "Local" {
		if loc, err := time.LoadLocation(name); err == nil {
			return loc, nil
		}
	}
	return nil, fmt.Errorf("unknown timezone %q: use an IANA zone such as Europe/Paris or UTC", name)
}
