// Package runner runs a remediation as a sequence of registered steps. The
// registry builds every step from the collaborators below, so the steps depend
// on ports and never on an adapter.
package runner

import (
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// Collaborators bundles the ports the steps are built from. NewRegistry hands
// each step constructor only the ones that step uses and never calls a method
// on any of them, so the zero value builds a registry.
type Collaborators struct {
	Source    ports.SourceReader
	Evidence  ports.EvidenceReader
	Artifacts ports.ArtifactWriter
	Logger    *slog.Logger
	// ProjectRepoPaths maps a project name to its repository path.
	ProjectRepoPaths map[string]string
	Locator          ports.NodeLocator
	Upstream         ports.UpstreamChangeReader
	Versions         ports.VersionReader
	Precedents       ports.PrecedentReader
	CandidateSource  ports.CandidateSourceReader
	// Archive fetches a whole repository checkout at a commit, for a fix that
	// has to search the tree rather than read one known file.
	Archive ports.RepoArchive
	// Patcher writes a draft's files over a checkout Archive fetched, for the
	// pre-flight checks that run against the patched repository.
	Patcher ports.CheckoutPatcher
	// ContractLocator finds which ODCS contract file in that checkout declares the
	// python node being fixed.
	ContractLocator ports.ContractLocator
	// ModelReferences reads a dbt project in that checkout for a fix that
	// renames a model: the files that name it and the names already taken.
	ModelReferences ports.ModelReferenceFinder
	// ContractInspector reads the node declarations out of an ODCS contract file
	// document, so the answer's effect on them can be compared against what
	// the repository held before it was applied.
	ContractInspector ports.ContractInspector
	// Packager packs a project's directory of ODCS contracts into the
	// odcs-bundle.json a release is submitted with.
	Packager ports.ContractPackager
	// ImagePackages reads the package list of the rejected release's image of
	// a project, which the bundle records.
	ImagePackages ports.ImagePackagesSource
	// CurrentContracts reads a node's current contract from the catalog.
	CurrentContracts ports.CurrentContractReader
	// Releases reads the original failing release's node-failure set.
	Releases ports.ReleaseReader
	// PriorAttempts reads the release's fix rows, so a later try's prompt
	// can show the attempts recorded with no remediation.
	PriorAttempts repository.AttemptLister
	// Tries reads every try the runner made at the release, so a later try's
	// prompt can show what earlier tries tried and why they were refused.
	Tries repository.TryLister
	// SQLDialect is the sqlglot dialect the warehouse this install runs
	// speaks, resolved from the operator's configured warehouse engine.
	SQLDialect string
	// ReleaseChecks submits release checks and reads their status.
	ReleaseChecks ports.ReleaseChecks
	// Clock tells the time.
	Clock ports.Clock
	// ReleaseCheckTimeout is how long a release check may be running, or
	// its status unreadable since GateInput.SubmittedAt (the start of the gate
	// step that submitted the checks), before the gate fails the try as timed
	// out.
	ReleaseCheckTimeout time.Duration
}
