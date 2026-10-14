// Package runplan chooses the tasks of a new run from the slice of the release
// the run reads. Each kind of run has a Selector: WholeDAG for a schedule's
// cron or triggered run, Rerun, Rebase, SingleNode and Seeds. A Selector reads
// only the topology.Graph it is given and returns a Plan: every task of the
// run, which of them are carried over from a source run as succeeded, and
// which pending tasks are ready to dispatch when the run is created. A
// selection that cannot make a valid run is refused with one of the package's
// errors, before anything is written.
//
// The package is pure: it reads no storage, keeps no state and knows nothing
// of the run aggregate, which takes a Plan when it starts a run.
package runplan
