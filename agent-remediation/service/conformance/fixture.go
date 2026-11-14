// Package conformance runs every remediation type an install lists, and the
// flow that lists them, through fixed scenarios, with the real runner and the
// real steps over fakes that answer from a recorded fixture. A listed type
// with no fixture fails, so no type can skip its tests. The fakes and the
// store come from the caller through Suite.NewWorld; this package decides what
// each scenario feeds them and what the run must have written.
package conformance

import (
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// Fixture is one recorded failure and everything the fakes answer for it: the
// heal trigger, the repository at the trigger's commit, the objects and the
// code bundle the steps read, the catalog's answers, the release's, the
// packaging tool's, and the model's scripted answers. A type fixture names the
// type it tests in Type, and its trigger makes one cluster of that type; a
// flow fixture names Flow and the flow Scenario it plays instead.
type Fixture struct {
	// File is where the fixture was read, for messages.
	File    string
	Version int

	Type     string
	Flow     string
	Scenario Scenario

	// Trigger is the heal trigger the scenario accepts.
	Trigger remediation.Trigger
	// DeclineBeforeModel is Trigger with the fixture's decline_before_model
	// patch applied to every node: a trigger one of the type's context steps
	// declines before the model is called. Nil when the fixture has none.
	DeclineBeforeModel *remediation.Trigger

	// ServiceRepoPaths maps a service name to its path inside the repository.
	ServiceRepoPaths map[string]string
	// RepoFiles is the repository at the trigger's commit, by
	// repository-relative path.
	RepoFiles map[string]string
	// Objects is what object storage holds, by URI: dbt logs and candidate
	// SQL.
	Objects map[string]string
	// Bundle is the release's code bundle, by node id.
	Bundle map[string]ports.CandidateSource
	// Catalog is what the catalog answers.
	Catalog Catalog
	// Release is what the failing release answers.
	Release Release
	// Packager is what the contract packaging tool answers.
	Packager Packager

	// Answers are the model's answers for every cluster whose target has no
	// MemberAnswers, by scenario.
	Answers Answers
	// MemberAnswers are the model's answers for the clusters whose target is
	// the named node: a shared-upstream member's own cluster after the
	// upstream cluster fell back, or one of a flow fixture's clusters.
	MemberAnswers map[string]Answers

	// GateErrors is the error a failed verification run reports per node.
	GateErrors map[string]string
}

// Answers are scripted model answers, one per model call, in call order. When
// a run calls the model more often than a list holds, the list starts over, so
// a two-call prompt answers each try the same way.
type Answers struct {
	// Pass is answered by a scenario whose draft passes pre-flight.
	Pass []ports.ProposeResult
	// Fail is an answer pre-flight refuses.
	Fail []ports.ProposeResult
	// Decline is an answer the model gives when it declines, such as a low
	// confidence or no edit at all.
	Decline []ports.ProposeResult
}

// Catalog is what the catalog's ports answer.
type Catalog struct {
	// Locations is where each node lives, by node id. A node with no entry
	// cannot be located.
	Locations map[string]Location
	// CurrentVersions is the version of each node that runs now, by node id.
	CurrentVersions map[string]ports.CurrentVersion
	// UpstreamChanges is what changed upstream of each node, by node id.
	UpstreamChanges map[string][]prompt.UpstreamChange
	// Precedents are the failure precedents every lookup returns.
	Precedents []prompt.Precedent
}

// Location is the file and service that declare a node.
type Location struct {
	FilePath string
	Service  string
}

// Release is what the failing release's reader answers.
type Release struct {
	// ImageTag is the image tag of every service in the release.
	ImageTag string
	// FailingNodes is the release's failing nodes with their errors.
	FailingNodes map[string]string
}

// Packager is what the contract packaging tool answers: Merged when it
// accepts a contract directory, and a refusal carrying Refuse when Refuse is
// set.
type Packager struct {
	Merged string
	Refuse string
}
