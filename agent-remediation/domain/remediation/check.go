package remediation

import (
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

// Finding is one problem a check found in a draft.
type Finding struct {
	Code    string
	Message string
	File    string
	Line    int
}

// CheckResult is the outcome of one check. It is waiting while the check has
// started verification runs and has not yet read their answer.
type CheckResult struct {
	Check    string
	Findings []Finding
	Waiting  bool
	RunIDs   []string
}

// Passed reports whether the check is finished and found nothing.
func (r CheckResult) Passed() bool {
	return !r.Waiting && len(r.Findings) == 0
}

// firstUnpassed returns the check that did not pass, if any.
func firstUnpassed(results []CheckResult) (CheckResult, bool) {
	for _, r := range results {
		if !r.Passed() {
			return r, true
		}
	}
	return CheckResult{}, false
}

// Checked is a draft every pre-flight check passed. Only NewChecked makes a
// non-zero one, so holding a Checked is proof the draft was checked.
type Checked struct {
	draft Draft
	ok    bool
}

// NewChecked wraps a draft that every one of results passed. It errors when
// any result did not pass.
func NewChecked(d Draft, results []CheckResult) (Checked, error) {
	if r, bad := firstUnpassed(results); bad {
		return Checked{}, fmt.Errorf("check %q did not pass", r.Check)
	}
	return Checked{draft: d, ok: true}, nil
}

// Draft returns the checked draft.
func (c Checked) Draft() Draft { return c.draft }

// IsZero reports whether c is the zero value, not made by NewChecked.
func (c Checked) IsZero() bool { return !c.ok }

// Verified is a proposal whose checked drafts passed the verification gate. Only
// NewVerified makes a non-zero one.
type Verified struct {
	proposalID    string
	verifications []proposal.Verification
	checked       []Checked
}

// NewVerified wraps the checked drafts of one proposal with the verification
// runs that judged them. It errors on an empty proposal id, no checked draft, a
// zero Checked, or a gate result that did not pass.
func NewVerified(proposalID string, checked []Checked, gate []CheckResult, verifications []proposal.Verification) (Verified, error) {
	if proposalID == "" {
		return Verified{}, errors.New("verified proposal has no id")
	}
	if len(checked) == 0 {
		return Verified{}, errors.New("verified proposal has no checked draft")
	}
	for _, c := range checked {
		if c.IsZero() {
			return Verified{}, errors.New("verified proposal holds a draft that was not checked")
		}
	}
	if r, bad := firstUnpassed(gate); bad {
		return Verified{}, fmt.Errorf("gate %q did not pass", r.Check)
	}
	return Verified{
		proposalID:    proposalID,
		verifications: append([]proposal.Verification(nil), verifications...),
		checked:       append([]Checked(nil), checked...),
	}, nil
}

// ProposalID is the id of the proposal the verified drafts belong to.
func (v Verified) ProposalID() string { return v.proposalID }

// Verifications returns the verification runs that judged the drafts.
func (v Verified) Verifications() []proposal.Verification {
	return append([]proposal.Verification(nil), v.verifications...)
}

// IsZero reports whether v is the zero value, not made by NewVerified.
func (v Verified) IsZero() bool { return v.proposalID == "" }
