// Package workflow defines the remediation flows and types as plain values: a
// flow names the steps that run around a model call and the types it may use,
// and a type says which failing clusters it handles and what its model call is
// given. The values carry no serialization detail; an adapter fills them from
// the install's files.
package workflow

import "slices"

// SupportedVersion is the only file version the definitions understand.
const SupportedVersion = 1

// Pos is a place in a file, kept so a problem names where it was written.
type Pos struct {
	File string
	Line int
}

// StepName is the name a step is registered under.
type StepName string

// StepRef is one use of a step in a flow or type, with where it was written.
type StepRef struct {
	Name StepName
	Pos  Pos
}

// Stage is a place in a remediation where steps run.
type Stage string

const (
	// StageContext gathers what the model is shown.
	StageContext Stage = "context"
	// StagePrompt builds the model's request.
	StagePrompt Stage = "prompt"
	// StagePreflight checks a draft before it is verified.
	StagePreflight Stage = "preflight"
	// StageCrossCluster checks the drafts of all clusters of a try together.
	StageCrossCluster Stage = "cross_cluster"
	// StageGate decides whether a try's drafts are verified.
	StageGate Stage = "gate"
	// StageApprover works out who must approve what a remediation did.
	StageApprover Stage = "approver"
	// StageOutput hands a result to its destination.
	StageOutput Stage = "output"
)

// DataClass is a kind of content that evidence put in front of the model
// can be. A flow lists the classes its model may see, and a step whose
// evidence is of another class cannot stand in that flow.
type DataClass string

const (
	// ClassCode is source code, SQL and contracts, and the differences between
	// versions of them.
	ClassCode DataClass = "code"
	// ClassLogs is the text that failures, tools and earlier tries report:
	// error output, logs, precedents and findings.
	ClassLogs DataClass = "logs"
	// ClassRows is rows of data held in a warehouse.
	ClassRows DataClass = "rows"
)

// DataClasses returns every class, in the order a message lists them.
func DataClasses() []DataClass {
	return []DataClass{ClassCode, ClassLogs, ClassRows}
}

// IsValid reports whether c is one of the classes.
func (c DataClass) IsValid() bool {
	return slices.Contains(DataClasses(), c)
}

// ClusterKind is the shape of a cluster of failing nodes.
type ClusterKind string

const (
	// ClusterSingle: one failing node.
	ClusterSingle ClusterKind = "single"
	// ClusterSharedUpstream: several nodes fixed at a shared changed ancestor.
	ClusterSharedUpstream ClusterKind = "shared_upstream"
	// ClusterCrossService: nodes of more than one service.
	ClusterCrossService ClusterKind = "cross_service"
)

// NodeTypeNone stands in a Match's node types for a failure that carries no
// node type.
const NodeTypeNone = "none"

// Agent is the model call of a type: the prompt that builds the request, the
// tools the model may use, and how many rounds it may take.
type Agent struct {
	Prompt StepRef
	Tools  []StepRef
	Rounds int
	Pos    Pos
}

// Type handles the clusters its Match selects.
type Type struct {
	Version   int
	Name      string
	Match     Match
	Context   []StepRef
	Agent     Agent
	Preflight []StepRef
	// BasedOn is the `<name>@<revision>` the file's based_on field holds: the
	// built-in type the file was copied from and the revision of it that was
	// copied. Empty when the file has no based_on.
	BasedOn string
	// Revision identifies the file's content when it was loaded.
	Revision string
	File     string
	Pos      Pos
}

// OutputAction is what an Output does with a result.
type OutputAction string

const (
	// OutputPullRequest opens a fix pull request.
	OutputPullRequest OutputAction = "pull_request"
	// OutputHandOver hands the failure to a person.
	OutputHandOver OutputAction = "hand_over"
)

// Output is where a flow sends a result.
type Output struct {
	Action OutputAction
	// Emits is the base name of the stream the output publishes on
	// ("remediation.proposed"), empty when it publishes nothing.
	Emits string
	Pos   Pos
}

// Flow is the sequence a remediation follows for one trigger.
type Flow struct {
	Version int
	Name    string
	Trigger string
	// Types lists the names of the types the flow may use, in priority order.
	Types          []string
	TypesPos       Pos
	Context        []StepRef
	AcrossClusters []StepRef
	Gate           []StepRef
	// Tries is how many times the flow drafts and verifies before it gives up.
	Tries    int
	OnPass   Output
	OnGiveUp Output
	// Approver is the step that works out who must approve what a remediation
	// did with each node, run before the outputs.
	Approver StepRef
	// ModelMaySee lists the classes of content the model may be shown under
	// this flow. A flow lists at least one.
	ModelMaySee []DataClass
	// ModelMaySeePos is where the list was written, or the flow's own position
	// when the file has none.
	ModelMaySeePos Pos
	// BasedOn is the `<name>@<revision>` the file's based_on field holds: the
	// built-in flow the file was copied from and the revision of it that was
	// copied. Empty when the file has no based_on.
	BasedOn string
	// Revision identifies the file's content when it was loaded.
	Revision string
	File     string
	Pos      Pos
}

// Set is every flow and type loaded from an install's files, by name.
type Set struct {
	Flows map[string]Flow
	Types map[string]Type
}

// TypeFor returns the type that handles a failing cluster under a flow: the
// first type the flow lists, in its order, whose Match selects the cluster. A
// type the flow does not list is never returned, and neither is a name the set
// does not define.
func (s Set) TypeFor(flow Flow, cluster ClusterKind, source, nodeType string) (Type, bool) {
	for _, name := range flow.Types {
		t, ok := s.Types[name]
		if ok && t.Match.Selects(cluster, source, nodeType) {
			return t, true
		}
	}
	return Type{}, false
}
