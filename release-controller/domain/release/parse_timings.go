package release

import (
	"maps"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// ParseTimings is what a parse result says about the parse of one check: how long
// the parse took, how long its contract stages took, and how many contracts the
// contract bundle holds by family (`dbt`, `python`; a family with none has no
// key). Each field says whether the parse reported it by being set. The zero
// value reports nothing: the check has not been parsed yet, or its producer does
// not report timings.
type ParseTimings struct {
	// ParseMs and ContractsMs are the milliseconds of the parse without its
	// contract stages and of the contract stages. They are set together or not at
	// all: NewParseTimings sets both or neither, and so does the repository when it
	// reads a check back.
	ParseMs     *int64
	ContractsMs *int64
	// ContractsByFamily is nil when the parse reported no count, and empty when it
	// counted a release with no contracts. Only an ok parse counts.
	ContractsByFamily map[model.ContractFamily]int
}

// NewParseTimings builds the timings a parse result carries from its decoded
// fields. pkg/events decodes a duration the entry does not name as 0 and a count
// it does not name as nil, so a missing duration and a reported 0 look alike.
// That decoding sets the compatibility rule applied here: two zero durations
// mean the producer does not report them, so both stay unset, and a parse that
// reports 0 for both reads as not reported too. Any other pair is kept as
// decoded, a zero beside a non-zero included. A nil count stays unset, and an
// empty one is kept as the count of a release with no contracts.
func NewParseTimings(parseMs, contractsMs int64, contractsByFamily map[model.ContractFamily]int) ParseTimings {
	t := ParseTimings{ContractsByFamily: maps.Clone(contractsByFamily)}
	if parseMs != 0 || contractsMs != 0 {
		t.ParseMs = &parseMs
		t.ContractsMs = &contractsMs
	}
	return t
}
