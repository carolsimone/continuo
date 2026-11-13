// Package runner runs a remediation as a sequence of registered steps. The
// registry builds every step from the collaborators below, so the steps depend
// on ports and never on an adapter.
package runner

import (
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// Collaborators bundles the ports the steps are built from. NewRegistry hands
// each step constructor only the ones that step uses and never calls a method
// on any of them, so the zero value builds a registry.
type Collaborators struct {
	Source    ports.SourceReader
	Evidence  ports.EvidenceReader
	Sanitizer ports.LogSanitizer
	Artifacts ports.ArtifactWriter
	Logger    *slog.Logger
	// ServiceRepoPaths maps a service name to its repository path.
	ServiceRepoPaths map[string]string
	Locator          ports.NodeLocator
	Upstream         ports.UpstreamChangeReader
	Versions         ports.VersionReader
	Precedents       ports.PrecedentReader
	CandidateSource  ports.CandidateSourceReader
	// Archive fetches a whole repository checkout at a commit, for a fix that
	// has to search the tree rather than read one known file.
	Archive ports.RepoArchive
	// ContractLocator finds which contract yaml in that checkout declares the
	// python node being fixed.
	ContractLocator ports.ContractLocator
	// ContractInspector reads the node declarations out of a contract yaml
	// document, so the answer's effect on them can be compared against what
	// the repository held before it was applied.
	ContractInspector ports.ContractInspector
	// Packager turns a directory of python-node contract yaml files into the
	// merged wire contract a release is submitted with.
	Packager ports.ContractPackager
	// Releases reads the original failing release's node-failure set.
	Releases ports.ReleaseReader
	// PriorAttempts reads the attempts already recorded for the failing node,
	// so a later attempt's prompt can show what earlier ones tried.
	PriorAttempts repository.AttemptLister
	// SQLDialect is the sqlglot dialect the warehouse this install runs
	// speaks, resolved from the operator's configured warehouse engine.
	SQLDialect string
	// Pipeline submits verification runs and reads their status.
	Pipeline ports.VerificationPipeline
	// Clock tells the time.
	Clock ports.Clock
}
