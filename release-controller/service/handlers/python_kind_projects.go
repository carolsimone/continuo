package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// PythonKindProjectsError reports that projects are still released as kind
// python (a contract.yaml project). topology-controller reads no contract.yaml,
// so a release of such a project would fail.
type PythonKindProjectsError struct {
	// Projects are the names of those projects, sorted ascending.
	Projects []string
}

func (e *PythonKindProjectsError) Error() string {
	return fmt.Sprintf("these projects are still released as kind %q: %s. "+
		"Release each one as %q (docs/contracts.md) on a continuo version that accepts it, then upgrade.",
		release.ManifestKindPython, strings.Join(e.Projects, ", "), release.ManifestKindPythonODCS)
}

// CheckNoProjectOnPythonKind refuses a release database whose service_prod
// still holds a project released as kind python, with an error that names every
// such project. release-controller runs it at startup, so it does not start on
// such a database.
func CheckNoProjectOnPythonKind(ctx context.Context, projects repository.ServiceProdRepository) error {
	names, err := projects.ProjectsWithKind(ctx, release.ManifestKindPython)
	if err != nil {
		return fmt.Errorf("list the projects released as kind %q: %w", release.ManifestKindPython, err)
	}
	if len(names) > 0 {
		return &PythonKindProjectsError{Projects: names}
	}
	return nil
}
