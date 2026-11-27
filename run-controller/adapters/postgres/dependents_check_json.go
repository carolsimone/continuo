package postgres

import (
	"encoding/json"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
)

// dependentsCheckColumn renders p as the value tasks.dependents_check stores: the
// JSON of pkgevents.DependentsCheck, the shape a dispatch carries, as text, so
// a COPY and a statement parameter both write it as jsonb; nil, which writes
// NULL, for a nil dependents check.
func dependentsCheckColumn(p *seedreload.DependentsCheck) (*string, error) {
	if p == nil {
		return nil, nil
	}
	b, err := json.Marshal(pkgevents.DependentsCheckOf(p))
	if err != nil {
		return nil, fmt.Errorf("encode the dependents check of %s: %w", p.Relation, err)
	}
	s := string(b)
	return &s, nil
}

// dependentsCheckFromColumn reads tasks.dependents_check back through
// pkgevents.DependentsCheck; NULL reads as nil.
func dependentsCheckFromColumn(s *string) (*seedreload.DependentsCheck, error) {
	if s == nil {
		return nil, nil
	}
	var p pkgevents.DependentsCheck
	if err := json.Unmarshal([]byte(*s), &p); err != nil {
		return nil, fmt.Errorf("decode a stored dependents check: %w", err)
	}
	return p.ToDomain(), nil
}

// dependentsCheckFindingsColumn renders f as the value
// task_attempts.dependents_check_findings stores: the JSON of
// pkgevents.DependentsCheckFindings, the shape a report carries, as text; nil,
// which writes NULL, for nil findings.
func dependentsCheckFindingsColumn(f *seedreload.Findings) (*string, error) {
	if f == nil {
		return nil, nil
	}
	b, err := json.Marshal(pkgevents.DependentsCheckFindingsOf(f))
	if err != nil {
		return nil, fmt.Errorf("encode dependents check findings: %w", err)
	}
	s := string(b)
	return &s, nil
}

// dependentsCheckFindingsFromColumn reads task_attempts.dependents_check_findings
// back through pkgevents.DependentsCheckFindings; NULL reads as nil.
func dependentsCheckFindingsFromColumn(s *string) (*seedreload.Findings, error) {
	if s == nil {
		return nil, nil
	}
	var f pkgevents.DependentsCheckFindings
	if err := json.Unmarshal([]byte(*s), &f); err != nil {
		return nil, fmt.Errorf("decode stored dependents check findings: %w", err)
	}
	return f.ToDomain(), nil
}

// dispatchSpecColumns are the columns of a task row every dispatch of the task
// carries. A read that scans them selects operation, plan_reason and
// dependents_check.
type dispatchSpecColumns struct {
	Operation       string  `db:"operation"`
	PlanReason      string  `db:"plan_reason"`
	DependentsCheck *string `db:"dependents_check"`
}

// spec is the run.DispatchSpec the columns store.
func (c dispatchSpecColumns) spec() (run.DispatchSpec, error) {
	check, err := dependentsCheckFromColumn(c.DependentsCheck)
	if err != nil {
		return run.DispatchSpec{}, err
	}
	return run.DispatchSpec{Operation: operationFromColumn(c.Operation), Reason: c.PlanReason, DependentsCheck: check}, nil
}
