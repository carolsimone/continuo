package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// PythonKindServicesError reports that services are still released as kind
// python (a contract.yaml service). topology-controller reads no contract.yaml,
// so a release of such a service would fail. Its text is the one release
// migration V30 raises.
type PythonKindServicesError struct {
	// Services are the names of those services, sorted ascending.
	Services []string
}

func (e *PythonKindServicesError) Error() string {
	return fmt.Sprintf("these services are still released as kind %q: %s. "+
		"Release each one as %q (docs/contracts.md) on the continuo version you run now, then upgrade.",
		release.ManifestKindPython, strings.Join(e.Services, ", "), release.ManifestKindPythonODCS)
}

// CheckNoServiceOnPythonKind refuses a release database whose service_prod
// still holds a service released as kind python, with an error that names every
// such service. release-controller runs it at startup, so it does not start on
// such a database even when the migration that raises the same error did not
// stop the rollout, as with the bundled Postgres.
func CheckNoServiceOnPythonKind(ctx context.Context, services repository.ServiceProdRepository) error {
	names, err := services.ServicesWithKind(ctx, release.ManifestKindPython)
	if err != nil {
		return fmt.Errorf("list the services released as kind %q: %w", release.ManifestKindPython, err)
	}
	if len(names) > 0 {
		return &PythonKindServicesError{Services: names}
	}
	return nil
}
