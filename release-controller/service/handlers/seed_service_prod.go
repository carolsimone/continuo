package handlers

import (
	"context"
	"fmt"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// SeedServiceProd derives one service_prod pointer per distinct service in the
// current_prod snapshot and upserts it. The manifest S3 key for each service is
// taken verbatim from existingKeys (service_name -> s3 key); a service present
// in the snapshot but absent from existingKeys is an error (we cannot point at
// a manifest we do not know). image_tag is the first non-empty tag among the
// service's nodes. manifest_kind is derived from the service's own nodes: any
// node whose NodeType.IsPython() is true (python-model or python-csv) makes
// the service ManifestKindPython, otherwise ManifestKindDbt; a service whose
// nodes mix python and non-python types is an error (a service is either a
// dbt project or a python service, never both). Idempotent: re-running
// upserts the same rows.
// Returns the count of services seeded.
func SeedServiceProd(
	ctx context.Context,
	cp *release.CurrentProd,
	existingKeys map[string]string,
	repo repository.ServiceProdRepository,
	now time.Time,
) (int, error) {
	// Group nodes by service name; track the first non-empty image tag and
	// which node kinds (dbt vs python) were seen, per service.
	type svcEntry struct {
		imageTag  string
		hasDbt    bool
		hasPython bool
	}
	entries := map[string]*svcEntry{}
	orderSeen := []string{} // deterministic iteration order

	for _, node := range cp.TopologySnapshot() {
		if node.ServiceName == "" {
			continue
		}
		if _, exists := entries[node.ServiceName]; !exists {
			entries[node.ServiceName] = &svcEntry{}
			orderSeen = append(orderSeen, node.ServiceName)
		}
		e := entries[node.ServiceName]
		if e.imageTag == "" && node.ImageTag != "" {
			e.imageTag = node.ImageTag
		}
		if pkg_model.NodeType(node.NodeType).IsPython() {
			e.hasPython = true
		} else {
			e.hasDbt = true
		}
	}

	// Validate every service has a manifest key AND an unambiguous kind BEFORE
	// writing anything, so either failure aborts the whole seed atomically
	// rather than leaving a partial set of pointers behind.
	for _, svc := range orderSeen {
		if _, ok := existingKeys[svc]; !ok {
			return 0, fmt.Errorf("no manifest S3 key provided for service %q", svc)
		}
		if entries[svc].hasDbt && entries[svc].hasPython {
			return 0, fmt.Errorf("service %q has both dbt and python nodes; a service must be a single kind", svc)
		}
	}

	for _, svc := range orderSeen {
		kind := release.ManifestKindDbt
		if entries[svc].hasPython {
			kind = release.ManifestKindPython
		}
		sp := release.NewServiceProd(svc, cp.ReleaseID(), existingKeys[svc], entries[svc].imageTag, kind, now)
		if err := repo.Upsert(ctx, sp); err != nil {
			return 0, fmt.Errorf("upsert service_prod for %q: %w", svc, err)
		}
	}

	return len(orderSeen), nil
}
