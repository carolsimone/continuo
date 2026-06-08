package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// SeedServiceProd derives one service_prod pointer per distinct service in the
// current_prod snapshot and upserts it. The manifest S3 key for each service is
// taken verbatim from existingKeys (service_name -> s3 key); a service present
// in the snapshot but absent from existingKeys is an error (we cannot point at
// a manifest we do not know). image_tag is the first non-empty tag among the
// service's nodes. Idempotent: re-running upserts the same rows.
// Returns the count of services seeded.
func SeedServiceProd(
	ctx context.Context,
	cp *release.CurrentProd,
	existingKeys map[string]string,
	repo repository.ServiceProdRepository,
	now time.Time,
) (int, error) {
	// Group nodes by service name; track the first non-empty image tag per service.
	type svcEntry struct {
		imageTag string
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
		if entries[node.ServiceName].imageTag == "" && node.ImageTag != "" {
			entries[node.ServiceName].imageTag = node.ImageTag
		}
	}

	// Validate every service has a manifest key BEFORE writing anything, so a
	// missing key fails the whole seed atomically rather than leaving a partial
	// set of pointers behind.
	for _, svc := range orderSeen {
		if _, ok := existingKeys[svc]; !ok {
			return 0, fmt.Errorf("no manifest S3 key provided for service %q", svc)
		}
	}

	for _, svc := range orderSeen {
		sp := release.NewServiceProd(svc, cp.ReleaseID(), existingKeys[svc], entries[svc].imageTag, now)
		if err := repo.Upsert(ctx, sp); err != nil {
			return 0, fmt.Errorf("upsert service_prod for %q: %w", svc, err)
		}
	}

	return len(orderSeen), nil
}
