package release

import "maps"

// ParseTimings is what a parse result says about the parse of one run: how long
// the parse took, how long its contract stages took, and how many contracts the
// contract bundle holds by family (`dbt`, `python`; a family with none has no
// key). Each field says whether the parse reported it by being set. The zero
// value reports nothing: the run has not been parsed yet, or its producer does
// not report timings.
type ParseTimings struct {
	// ParseMs and ContractsMs are the milliseconds of the parse without its
	// contract stages and of the contract stages. They are set together or not at
	// all.
	ParseMs     *int64
	ContractsMs *int64
	// ContractsByFamily is nil when the parse reported no count, and empty when it
	// counted a release with no contracts. Only an ok parse counts.
	ContractsByFamily map[string]int
}

// NewParseTimings builds the timings a parse result carries from its decoded
// fields, which read as zero or nil when the entry does not name them. Two zero
// durations mean the producer does not report them, so both stay unset; any
// other pair is kept as decoded, a zero beside a non-zero included. A nil count
// stays unset, and an empty one is kept as the count of a release with no
// contracts.
func NewParseTimings(parseMs, contractsMs int64, contractsByFamily map[string]int) ParseTimings {
	t := ParseTimings{ContractsByFamily: maps.Clone(contractsByFamily)}
	if parseMs != 0 || contractsMs != 0 {
		t.ParseMs = &parseMs
		t.ContractsMs = &contractsMs
	}
	return t
}
