package run

import "errors"

var (
	// ErrNodeAlreadyTerminal is returned when CompleteNode is called on a node
	// that has already reached a terminal status. Callers should treat this as
	// idempotent re-delivery and return nil.
	ErrNodeAlreadyTerminal = errors.New("run: node is already in a terminal status")

	// ErrNodeNotInScope is returned when the target node is absent from the
	// loaded subgraph — the node is not part of the run's projected :EXECUTES set.
	ErrNodeNotInScope = errors.New("run: node not found in loaded subgraph")

	// ErrVersionConflict is returned by AggregateRepository.Save when the
	// aggregate's Version no longer matches the persisted value. Callers
	// must reload and retry.
	ErrVersionConflict = errors.New("run: version conflict — reload and retry")
)
