package postgres

import (
	"encoding/json"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
)

// seedPreflightColumn renders p as the value tasks.seed_preflight stores: the
// JSON of pkgevents.SeedPreflight, the shape a dispatch carries, as text, so
// a COPY and a statement parameter both write it as jsonb; nil, which writes
// NULL, for a nil preflight.
func seedPreflightColumn(p *seedreload.Preflight) (*string, error) {
	if p == nil {
		return nil, nil
	}
	b, err := json.Marshal(pkgevents.SeedPreflightOf(p))
	if err != nil {
		return nil, fmt.Errorf("encode the seed preflight of %s: %w", p.Relation, err)
	}
	s := string(b)
	return &s, nil
}

// seedPreflightFromColumn reads tasks.seed_preflight back through
// pkgevents.SeedPreflight; NULL reads as nil.
func seedPreflightFromColumn(s *string) (*seedreload.Preflight, error) {
	if s == nil {
		return nil, nil
	}
	var p pkgevents.SeedPreflight
	if err := json.Unmarshal([]byte(*s), &p); err != nil {
		return nil, fmt.Errorf("decode a stored seed preflight: %w", err)
	}
	return p.ToDomain(), nil
}

// seedPreflightFindingsColumn renders f as the value
// task_attempts.seed_preflight_findings stores: the JSON of
// pkgevents.SeedPreflightFindings, the shape a report carries, as text; nil,
// which writes NULL, for nil findings.
func seedPreflightFindingsColumn(f *seedreload.Findings) (*string, error) {
	if f == nil {
		return nil, nil
	}
	b, err := json.Marshal(pkgevents.SeedPreflightFindingsOf(f))
	if err != nil {
		return nil, fmt.Errorf("encode seed preflight findings: %w", err)
	}
	s := string(b)
	return &s, nil
}

// seedPreflightFindingsFromColumn reads task_attempts.seed_preflight_findings
// back through pkgevents.SeedPreflightFindings; NULL reads as nil.
func seedPreflightFindingsFromColumn(s *string) (*seedreload.Findings, error) {
	if s == nil {
		return nil, nil
	}
	var f pkgevents.SeedPreflightFindings
	if err := json.Unmarshal([]byte(*s), &f); err != nil {
		return nil, fmt.Errorf("decode stored seed preflight findings: %w", err)
	}
	return f.ToDomain(), nil
}

// dispatchSpecColumns are the columns of a task row every dispatch of the task
// carries. A read that scans them selects operation, plan_reason and
// seed_preflight.
type dispatchSpecColumns struct {
	Operation     string  `db:"operation"`
	PlanReason    string  `db:"plan_reason"`
	SeedPreflight *string `db:"seed_preflight"`
}

// spec is the run.DispatchSpec the columns store.
func (c dispatchSpecColumns) spec() (run.DispatchSpec, error) {
	pf, err := seedPreflightFromColumn(c.SeedPreflight)
	if err != nil {
		return run.DispatchSpec{}, err
	}
	return run.DispatchSpec{Operation: operationFromColumn(c.Operation), Reason: c.PlanReason, Preflight: pf}, nil
}
